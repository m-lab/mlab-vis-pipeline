#!/usr/bin/env python
'''

'''
import os
import copy
import sqlparse
from search_table_utils import *

from create_bigtable_time_configs import \
    read_text, save_text, read_json, save_json, \
    get_query_relative_filename, get_query_full_filename, \
    BIGQUERY_DATE_TABLE, CONFIG_DIR
from configurations.search_tables import AGGREGATIONS, LOCATION_LEVELS, \
    TIME_RANGES, TEST_DATE_COMPARISONS, HISTOGRAM_BINS, LOCATION_CLIENT_ASN_LEVELS

# -- Templates:
# Location lists:
LOCATION_LIST_SUBSELECT_TEMPLATE = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "templates", "location_list_sub_select.sql")

LOCATION_LIST_LEFTJOIN_TEMPLATE = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "templates", "location_list_left_join.sql")

# Location search:
LOCATION_SEARCH_JOIN_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                             "templates", "location_search_left_join_section.sql")
SEARCH_JSON_TEMPLATE = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "templates", "search_template.json")
# location client asn number list:
LOCATION_CLIENT_ASN_LEFTJOIN_TEMPLATE = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "templates", "location_client_asn_number_left_join.sql")

LOCATION_CLIENT_ASN_SUBSELECT_TEMPLATE= os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "templates", "location_client_asn_number_sub_select.sql")

##########################
# Location client asn list
##########################

def build_location_client_asn_number_list():
    print("client_location_client_asn_list")
    build_location_client_asn_number_list_sql()
    build_location_client_asn_number_list_json()

def build_location_client_asn_number_list_sql():
    '''
    Builds the sql query for the location_client_asn_list table
    '''

    subselect_template = read_text(LOCATION_CLIENT_ASN_SUBSELECT_TEMPLATE)
    leftjoin_template = read_text(LOCATION_CLIENT_ASN_LEFTJOIN_TEMPLATE)

    config = AGGREGATIONS["client_location_client_asn_list"]

    query_str = """
        select
        {0},
        "{1}" as speed_mbps_bins,
        {2}
        from
    """

    # add select fields:
    select_fields = "%s, \n %s, \n %s" % (
        list_fields(config["key_fields"]),
        list_fields(config["fields"]),
        timed_list_fields(config["timed_fields"], TIME_RANGES))

    # add histogram bins for reference
    bins = ",".join(str(b) for b in HISTOGRAM_BINS)

    # add concatenated histogram bins
    bin_fields = []
    for timename in TIME_RANGES:
        for field in config["binned_fields"]:
            name = field_name(field)
            bin_fields.append("{0}_{1}_bins".format(timename, name))


    query_str = query_str.format(select_fields, bins, ",\n".join(bin_fields))

    # handle sub queries
    subqueries = []

    for location_level in LOCATION_CLIENT_ASN_LEVELS:

        location_type = location_level["type"]

        # build internal key
        # If large list, concat.
        if (isinstance(config["region_key_fields"][location_type], list) and
                len(config["region_key_fields"][location_type]) > 1):

            key_str = replace(
                lower(
                    concat(config["region_key_fields"][location_type], "all"))
                , " ", "")
        # If single item, just ifnull and lower single string
        elif (isinstance(config["region_key_fields"][location_type], list) and
              len(config["region_key_fields"][location_type]) == 1):

            key_str = "LOWER(IFNULL(all.{0}, \"\"))".format(
                config["region_key_fields"][location_type][0])
        # If no key, use emptry string.
        else:
            key_str = "\"\""

        key_str += " as {0}".format(config["key_name"])

        left_joins = ""

        # for each time granularity, add a left join
        for time_comparison in TEST_DATE_COMPARISONS:

            # compute histogram fields
            binned_fields = []
            for field in config["binned_fields"]:
                binned_fields.append(output_bin_string(field, HISTOGRAM_BINS))

            left_joins += leftjoin_template.format(

                # 0 - selected fields
                list_fields(location_level["fields"]),

                # 1 - time comparison
                TEST_DATE_COMPARISONS[time_comparison],

                # 2 - location field not null
                location_level["location_field"],

                # 3 - all fields
                join_on_fields(location_level["fields"], time_comparison),

                # 4 - name of sub table
                time_comparison,

                # 5 - bins
                ", \n".join(binned_fields)
            )

        # add binned timed fields
        binned_timed_fields = []
        groupby_binned_timed_fields = []
        for time_comparison in TEST_DATE_COMPARISONS:
            for field in config["binned_fields"]:
                name = field_name(field)
                binned_timed_fields.append("{0}.{1}_bins as {0}_{1}_bins"
                    .format(time_comparison, name))
                groupby_binned_timed_fields.append("{0}_{1}_bins"
                    .format(time_comparison, name))

        # add sub select
        subqueries.append(
            subselect_template.format(
                # 0 - new key string
                key_str,

                # 1 - type of location
                location_level["type"],

                # 2 - all.field as fields
                all_table_fields(location_level["fields"]),

                # 3 - timed data fields
                all_table_fields(config["timed_fields"], TIME_RANGES, True) +
                ",\n" + ", \n".join(binned_timed_fields),

                # 4 - left joins
                left_joins,

                # 5 - group by
                list_fields(config["key_fields"]) + ",\n" +
                list_fields(location_level["fields"]) + ",\n" +
                list_fields(config["timed_fields"], TIME_RANGES) + ", \n" +
                list_fields(groupby_binned_timed_fields)
            )
        )

    query_str += ", \n".join(subqueries)

    save_text(get_query_full_filename(config["table_name"]), query_str)

def setup_base_json(config_name, config):
    '''
    Creates base json structure for a search table
    '''

    json_struct = read_json(SEARCH_JSON_TEMPLATE)
    config = AGGREGATIONS[config_name]

    # add key fields
    json_struct["row_keys"] = copy.copy(config["key_fields"])

    # add bigquery file
    json_struct["bigquery_queryfile"] = get_query_relative_filename(config["table_name"])

    # big query table name
    json_struct["bigtable_table_name"] = config["table_name"]
    json_struct["bigquery_table"] =  BIGQUERY_DATE_TABLE

    # key field
    # For tables with no time aggregation,
    # the key field should match the table name
    json_struct["key"] = config["table_name"]

    # add columns
    json_struct["columns"] = config["key_fields"]
    json_struct["columns"] += config["fields"]

    return json_struct

def add_timed_columns_json(json_struct, config):
    '''
    Adds columns to a json table structure that have a time
    prefix.
    '''
    timed_fields = []
    for timename in TEST_DATE_COMPARISONS:
        for field in config["timed_fields"]:
            name = field_name(field)

            f = copy.copy(field)
            f["name"] = timename + "_" + name
            timed_fields.append(f)

    json_struct["columns"] += timed_fields

    return json_struct

def add_binned_columns(json_struct, config):
    '''
    Adds columns to a json table structure that have time
    prefixed bin columns for a set of fields
    '''
    timed_binned_columns = []
    for timename in TEST_DATE_COMPARISONS:
        for field in config["binned_fields"]:
            name = field_name(field)
            f = copy.copy(field)

            f["name"] = "{0}_{1}_bins".format(timename, name)
            timed_binned_columns.append(f)

    json_struct["columns"] += timed_binned_columns
    return json_struct

def build_location_client_asn_number_list_json():
    '''
    Builds the client_location_client_asn_list table json structure
    for big table
    '''
    config = AGGREGATIONS["client_location_client_asn_list"]
    json_struct = setup_base_json("client_location_client_asn_list", config)

    # add timed columns
    json_struct = add_timed_columns_json(json_struct, config)

    # add binned columns
    json_struct = add_binned_columns(json_struct, config)

    config_filepath = os.path.join(CONFIG_DIR, config["table_name"] + ".json")
    save_json(config_filepath, json_struct)

#################
# Location List
# ###############
def build_location_list():
    print("client_location_list")
    build_location_list_sql()
    build_location_list_json()

def build_location_list_json():
    '''
        Builds the location_list table json structure
        for big table
    '''
    config = AGGREGATIONS["client_location_list"]
    json_struct = setup_base_json("client_location_list", config)

    # add timed columns
    json_struct = add_timed_columns_json(json_struct, config)

    config_filepath = os.path.join(CONFIG_DIR, config["table_name"] + ".json")
    save_json(config_filepath, json_struct)

def build_location_list_sql():
    '''
        Builds the client_location_list table sql query file
        for big table
    '''
    subselect_template = read_text(LOCATION_LIST_SUBSELECT_TEMPLATE)
    leftjoin_template = read_text(LOCATION_LIST_LEFTJOIN_TEMPLATE)

    config = AGGREGATIONS["client_location_list"]
    query_str = """
        select
        {0}
        from
    """

    # add select fields:
    select_fields = "%s, \n %s, \n %s, \n %s" % (
        list_fields(config["key_fields"]),
        "type",
        list_fields(LOCATION_LEVELS[0]["fields"]),
        timed_list_fields(config["timed_fields"], TIME_RANGES))

    query_str = query_str.format(select_fields)

    subqueries = []

    for location_level in LOCATION_LEVELS:

        location_type = location_level["type"]

        # build internal key
        # If large list, concat.
        if (isinstance(config["region_key_fields"][location_type], list) and
                len(config["region_key_fields"][location_type]) > 1):

            key_str = replace(
                lower(
                    concat(config["region_key_fields"][location_type], "all"))
                , " ", "")

        # If single item, just ifnull and lower single string
        elif (isinstance(config["region_key_fields"][location_type], list) and
              len(config["region_key_fields"][location_type]) == 1):

            key_str = "LOWER(IFNULL(all.{0}, \"\"))".format(
                config["region_key_fields"][location_type][0])

        # If no key, use emptry string.
        else:
            key_str = "\"\""

        key_str += " as {0}".format(config["key_name"])

        left_joins = ""

        # for each time granularity, add a left join
        for time_comparison in TEST_DATE_COMPARISONS:
            left_joins += leftjoin_template.format(

                # 0 - selected fields
                list_fields(location_level["fields"]),

                # 1 - time comparison
                TEST_DATE_COMPARISONS[time_comparison],

                # 2 - location field not null
                location_level["location_field"],

                # 3 - all fields
                join_on_fields(location_level["fields"], time_comparison),

                # 4 - name of sub table
                time_comparison
            )

        # add sub select
        subqueries.append(
            subselect_template.format(
                # 0 - new key string
                key_str,

                # 1 - child location name
                location_level["location_field"],

                # 2 - type of location
                location_level["type"],

                # 3 - all.field as fields
                all_table_fields(location_level["fields"]),

                # 4 - timed data fields
                all_table_fields(config["timed_fields"], TIME_RANGES, True),

                # 5 - left joins
                left_joins,

                # 6 - group by
                list_fields(config["key_fields"]) + ",\n" +
                    list_fields(location_level["fields"]) + ",\n" +
                    list_fields(config["timed_fields"], TIME_RANGES)
            )
        )

    query_str += ", \n".join(subqueries)

    save_text(get_query_full_filename(config["table_name"]),
            sqlparse.format(query_str, strip_whitespace=False, reindent=True))

####
# Location search table sql and json:
# location_search
###
def build_location_search():
    print("location_search")
    build_location_search_sql()
    build_location_search_json()

def build_location_search_json():

    config = AGGREGATIONS["client_location_search"]
    json_struct = setup_base_json("client_location_search", config)

    config_filepath = os.path.join(CONFIG_DIR, config["table_name"] + ".json")
    save_json(config_filepath, json_struct)

def build_location_search_sql():
    # read template
    join_template = read_text(LOCATION_SEARCH_JOIN_TEMPLATE)
    config = AGGREGATIONS["client_location_search"]

    # build initial sql
    key_str = replace(
        lower(
            concat(config["key_columns"])),
            " ", "") + " as {0}".format(config["key_name"])

    # build list of fields
    fields = list_fields(config["fields"])

    # build query
    query_str = '''
    SELECT {0},
    {1}
    from
    '''.format(key_str, fields)

    segments = []
    for location_level in LOCATION_LEVELS:
        segments.append(join_template.format(

            # 0 - type
            location_level["type"],

            # 1 - selected fields
            list_fields(location_level["fields"]),

            # 2 - all.field as field selectors
            all_table_fields(location_level["fields"]),

            # 3 - join on fields
            join_on_fields(location_level["fields"], "threemonths"),

            # 4 - join table name
            "threemonths",

            # 5 - location field
            location_level["location_field"]
        ))

    query_str += ", \n".join(segments) + '''
    WHERE
        location IS NOT NULL;
    '''

    save_text(get_query_full_filename(config["table_name"]),
            sqlparse.format(query_str, strip_whitespace=False, reindent=True))

def main():
    '''
    Builds available tables
    '''
    build_location_client_asn_number_list()
    build_location_search()
    build_location_list()

main()
