#!/usr/bin/env python
'''
Create configs and query files for bigtable tables that have time
as the end parameter.
'''

import json
import os
import sqlparse

BIGQUERY_DATE_TABLE = "[mlab-sandbox:data_viz.all_ip_by_day]"
BIGQUERY_HOUR_TABLE = "[mlab-sandbox:data_viz.all_ip_by_hour]"

CUR_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))
QUERY_BASEDIR = os.path.join(CUR_DIR, "..", "..", "dataflow", "data", "bigtable", "queries")
CONFIG_QUERY_BASEDIR = os.path.join(".", "data", "bigtable", "queries")

TEMPLATE_DIR = os.path.join(CUR_DIR, "templates")
DATE_CONFIG_TEMPLATE_FILENAME = os.path.join(TEMPLATE_DIR, "time_config_template.json")


RESERVED_WORDS = ['date', 'hour']

TYPE_COLUMN = {"name": "type", "type": "string", "family": "meta"}

METRIC_FIELDS = {
    "download_speed_mbps_median": {
        "field": {"name": "download_speed_mbps_median", "type": "double", "family": "data"},
        "query": "nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median"
    },
    "upload_speed_mbps_median": {
        "field": {"name": "upload_speed_mbps_median", "type": "double", "family": "data"},
        "query": "nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median"
    },
    "rtt_avg": {
        "field": {"name": "rtt_avg", "type": "double", "family": "data"},
        "query": "SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg"
    },
    "retransmit_avg": {
        "field": {"name": "retransmit_avg", "type": "double", "family": "data"},
        "query": "AVG(packet_retransmit_rate) AS retransmit_avg"
    },
    "count": {
        "field": {"name": "count", "type": "integer", "family": "data"},
        "query": "COUNT(*) AS count"
    }
}

LOCATION_LEVELS = {
    "city": {
        "keys": [
            {"name": "client_continent_code", "type": "string", "family": "meta"},
            {"name": "client_country_code", "type": "string", "family": "meta"},
            {"name": "client_region_code", "type": "string", "family": "meta"},
            {"name": "client_city", "type": "string", "family": "meta"}

        ],
        "fields": [
            {"name": "client_continent", "type": "string", "family": "meta"},
            {"name": "client_continent_code", "type": "string", "family": "meta"},
            {"name": "client_country", "type": "string", "family": "meta"},
            {"name": "client_country_code", "type": "string", "family": "meta"},
            {"name": "client_region", "type": "string", "family": "meta"},
            {"name": "client_region_code", "type": "string", "family": "meta"},
            {"name": "client_city", "type": "string", "family": "meta"}
        ]
    },
    "region": {
        "keys": [
            {"name": "client_continent_code", "type": "string", "family": "meta"},
            {"name": "client_country_code", "type": "string", "family": "meta"},
            {"name": "client_region_code", "type": "string", "family": "meta"},

        ],
        "fields": [
            {"name": "client_continent", "type": "string", "family": "meta"},
            {"name": "client_continent_code", "type": "string", "family": "meta"},
            {"name": "client_country", "type": "string", "family": "meta"},
            {"name": "client_country_code", "type": "string", "family": "meta"},
            {"name": "client_region", "type": "string", "family": "meta"},
            {"name": "client_region_code", "type": "string", "family": "meta"}
        ]
    },
    "country": {
        "keys": [
            {"name": "client_continent_code", "type": "string", "family": "meta"},
            {"name": "client_country_code", "type": "string", "family": "meta"},

        ],
        "fields": [
            {"name": "client_continent", "type": "string", "family": "meta"},
            {"name": "client_continent_code", "type": "string", "family": "meta"},
            {"name": "client_country", "type": "string", "family": "meta"},
            {"name": "client_country_code", "type": "string", "family": "meta"}
        ]
    },
    "continent": {
        "keys": [
            {"name": "client_continent_code", "type": "string", "family": "meta"},

        ],
        "fields": [
            {"name": "client_continent", "type": "string", "family": "meta"},
            {"name": "client_continent_code", "type": "string", "family": "meta"}
        ]
    }
}

TABLE_BASES = {
    "server_asn_client_asn_client_loc": {
        "keys": [
            {"name": "client_location_key", "length": 50, "type": "string", "family": "meta"},
            {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
            {"name": "server_asn_number", "length": 40, "type": "string", "family": "meta"}
        ],
        "fields": [
            {"name": "server_asn_name", "type": "string", "family": "meta"},
            {"name": "client_asn_name", "type": "string", "family": "meta"}
        ]
    },
    "server_asn_client_loc": {
        "keys": [
            {"name": "client_location_key", "length": 50, "type": "string", "family": "meta"},
            {"name": "server_asn_number", "length": 40, "type": "string", "family": "meta"}
        ],
        "fields": [
            {"name": "server_asn_name", "type": "string", "family": "meta"}
        ]
    },

    "client_asn_client_loc": {
        "keys": [
            {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
            {"name": "client_location_key", "length": 50, "type": "string", "family": "meta"},
        ],
        "fields": [
            {"name": "client_asn_name", "type": "string", "family": "meta"}
        ]
    },
    "client_loc": {
        "keys": [
            {"name": "client_location_key", "length": 50, "type": "string", "family": "meta"},
        ],
        "fields": [
        ]
    },

    # OTHERS
    "client_asn": {
        "keys": [
            {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"}
        ],

        "fields": [
            {"name": "client_asn_name", "type": "string", "family": "meta"}
        ]
    },
    "server_asn": {
        "keys": [
            {"name": "server_asn_number", "length": 40, "type": "string", "family": "meta"},
        ],

        "fields": [
            {"name": "server_asn_name", "type": "string", "family": "meta"}
        ]
    },
    "server_asn_client_asn": {
        "keys":[
            {"name": "server_asn_number", "length": 40, "type": "string", "family": "meta"},
            {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"}
        ],
        "fields": [
            {"name": "server_asn_name", "type": "string", "family": "meta"},
            {"name": "client_asn_name", "type": "string", "family": "meta"}
        ]
    }
}


AGGREGATION_FILTERS = {
    # SERVER ASN NAME x CLIENT ASN NUMBER x CLIENT LOCATION
    "server_asn_client_asn_client_loc": [
        # "LENGTH(client_location_key) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0"
    ],

    # CLIENT ASN NUMBER x CLIENT LOCATION
    "client_asn_client_loc": [
        # "LENGTH(client_location_key) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0"
    ],

    # OTHER
    "client_asn": [
        "LENGTH(client_asn_number) > 0"
    ],
    "server_asn": [
        "LENGTH(server_asn_number) > 0"
    ],
    "server_asn_client_asn": [
        "LENGTH(client_asn_number) > 0",
        "LENGTH(server_asn_number) > 0"
    ]
}

DATE_AGGEGATIONS = {
    "day": [
        {"name": "date", "length": 10, "type": "string", "family": "meta"}
    ],
    "month": [
        {"name": "date", "length": 10, "type": "string", "family": "meta"}
    ],
    "year": [
        {"name": "date", "length": 10, "type": "string", "family": "meta"}
    ],
    "day_hour": [
        {"name": "date", "length": 10, "type": "string", "family": "meta"},
        {"name": "hour", "length": 10, "type": "string", "family": "meta"}
    ],
    "month_hour": [
        {"name": "date", "length": 10, "type": "string", "family": "meta"},
        {"name": "hour", "length": 10, "type": "string", "family": "meta"}
    ],
    "year_hour": [
        {"name": "date", "length": 10, "type": "string", "family": "meta"},
        {"name": "hour", "length": 10, "type": "string", "family": "meta"}
    ]
}

DATE_QUERIES = {
    "day": {"date": "DATE(local_test_date) AS date"},
    "month": {"date": "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), \"%Y-%m\") as date"},
    "year": {"date": "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), \"%Y\") as date"},
    "day_hour": {
        "date": "DATE(local_test_date) AS date",
        "hour": "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), \"%H\") as hour"
    },
    "month_hour": {
        "date": "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), \"%Y-%m\") as date",
        "hour": "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), \"%H\") as hour"
    },
    "year_hour": {
        "date": "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), \"%Y\") as date",
        "hour": "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), \"%H\") as hour"
    }
}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../", "dataflow"))

CONFIG_DIR = os.path.join(BASE_DIR, "data", "bigtable")
QUERY_DIR = os.path.abspath(os.path.join(CONFIG_DIR, "queries"))


def read_json(filename):
    '''
    read json
    '''
    data = {}
    with open(filename) as data_file:
        data = json.load(data_file)
    return data


def save_json(filename, data):
    '''
    save json
    '''
    with open(filename, 'w') as out_file:
        json.dump(data, out_file, indent=2, sort_keys=False)


def read_text(filename):
    '''
    read plain text
    '''
    text = ""
    with open(filename) as text_file:
        text = text_file.read()
    return text


def save_text(filename, text):
    '''
    save plain text
    '''
    with open(filename, 'w') as out_file:
        out_file.write(text)


def get_query_relative_filename(table_name):
    '''
    output relative path to query filename
    '''
    filename = "{0}.sql".format(table_name)
    full_path = os.path.join(CONFIG_QUERY_BASEDIR, filename)
    return full_path

def get_query_full_filename(table_name):
    '''
    output relative path to query filename
    '''
    filename = "{0}.sql".format(table_name)
    full_path = os.path.join(QUERY_BASEDIR, filename)
    return full_path


def get_table_name(aggregation_id, date_id):
    '''
    output bigtable table name
    '''
    if date_id:
        return "{0}_by_{1}".format(aggregation_id, date_id)
    else:
        return "{0}_list".format(aggregation_id)

def get_escaped_name(name):
    if name in RESERVED_WORDS:
        return "[{0}]".format(name)
    else:
        return name

def get_selection_for(name, date_id=None):
    if date_id and name in DATE_QUERIES[date_id].keys():
        return DATE_QUERIES[date_id][name]
    elif name in METRIC_FIELDS.keys():
        return METRIC_FIELDS[name]["query"]
    else:
        return name


def get_selection_for_type(type_name):
    return "\"{0}\" as type".format(type_name)

def build_subselection_query(selection_fields, group_by_fields, filters):
    query = "SELECT "
    query += ",\n".join(selection_fields)

    query += "\n FROM {0}\n"

    if len(filters) > 0:
        query += "WHERE \n"
        query += " AND \n".join(filters)
    query += "GROUP BY \n"
    query += ",\n".join(group_by_fields)

    return query

def get_filters(fields):
    '''
    create a series of WHERE filters for checking length
    '''
    filters = ["LENGTH({0}) > 0".format(f['name']) for f in fields]
    return filters

def get_fields(aggregation_options,
               date_id,
               date_config_fields):
    '''
    create selection field and group by field lists
    '''
    if not date_id:
        date_config_fields = []

    group_by_fields = []
    group_by_fields += [f['name'] for f in aggregation_options['keys']]
    group_by_fields += [f['name'] for f in aggregation_options['fields']]
    group_by_fields += [f['name'] for f in date_config_fields]

    selection_fields = [get_selection_for(name, date_id) for name in group_by_fields]
    selection_fields += [get_selection_for(name) for name in METRIC_FIELDS.keys()]

    selection_fields = [get_escaped_name(name) for name in selection_fields]

    group_by_fields = [get_escaped_name(name) for name in group_by_fields]

    return (selection_fields, group_by_fields)

# HACK
def add_location_key(subselection_fields, level_key_fields):
    '''
    Quick hack to get location key
    '''

    key = "REPLACE(LOWER(CONCAT("
    key = key + ", ".join(["IFNULL({0}, \"\")".format(field['name']) for field in level_key_fields])
    key = key + ", \"\")), \" \", \"\") AS client_location_key"

    return [key if x == "client_location_key" else x for x in subselection_fields]



def get_subselections(selection_fields,
                      group_by_fields,
                      filter_fields,
                      location_levels):
    '''
    output subselections
    '''


    group_by_fields.insert(0, 'type')

    subselection_queries = []

    for level_name, level_info in location_levels.iteritems():
        subselection_fields = selection_fields[:]
        subselection_fields = add_location_key(subselection_fields, level_info['keys'])
        subselection_group_by_fields = group_by_fields[:]
        subselection_fields.insert(0, get_selection_for_type(level_name))
        field_names = [f['name'] for f in level_info['fields']]
        subselection_fields += field_names
        subselection_group_by_fields += field_names

        filters = filter_fields[:]

        filters += get_filters(level_info['fields'])


        subselection_queries.append(build_subselection_query(subselection_fields, subselection_group_by_fields, filters))
    return subselection_queries

def get_shell_query(aggregation_options,
                    date_id,
                    date_config_fields,
                    location_levels):
    if not date_id:
        date_config_fields = []

    # TODO: type - what to do about type?
    select_fields = ['type']
    select_fields += [f['name'] for f in aggregation_options['keys']]
    select_fields += [f['name'] for f in aggregation_options['fields']]
    select_fields += [f['name'] for f in date_config_fields]
    select_fields += [name for name in METRIC_FIELDS.keys()]
    if len(location_levels) > 0:
        select_fields += [f['name'] for f in get_unique_location_levels(location_levels)]

    select_fields = [get_escaped_name(name) for name in select_fields]
    query = 'SELECT \n'
    query += ",\n".join(select_fields)
    query += "\n FROM \n"
    return query


def get_unique_location_levels(all_location_levels):
    uniques = []
    unique_names = []
    for level_options in all_location_levels.itervalues():
        for level_field in level_options['fields']:
            if level_field['name'] not in unique_names:
                uniques.append(level_field)
                unique_names.append(level_field['name'])
    return uniques


def create_config_file(aggregation_id,
                       aggregation_options,
                       date_id,
                       date_config_fields,
                       location_levels):
    '''
    Creates bigtable config files.
    '''
    # compute some names
    bigtable_table_name = get_table_name(aggregation_id, date_id)
    query_filename = get_query_relative_filename(bigtable_table_name)

    # get template
    base_config = read_json(DATE_CONFIG_TEMPLATE_FILENAME)

    location_columns = get_unique_location_levels(location_levels)

    if date_id:
        date_names = [x["name"] for x in date_config_fields]

        # fill in template
        if "hour" in date_names:
            base_config["bigquery_table"] = BIGQUERY_HOUR_TABLE
        else:
            base_config["bigquery_table"] = BIGQUERY_DATE_TABLE
    else:
        base_config = read_json(DATE_CONFIG_TEMPLATE_FILENAME)
        date_config_fields = []

    base_config["bigquery_queryfile"] = query_filename
    base_config["bigtable_table_name"] = bigtable_table_name
    base_config["key"] = aggregation_id
    base_config["frequency"] = date_id

    # row keys
    base_config["row_keys"] = aggregation_options['keys'] + date_config_fields

    # columns
    base_config["columns"] = base_config["columns"] + aggregation_options['keys']
    base_config["columns"] = base_config["columns"] + aggregation_options['fields']
    base_config["columns"] = base_config["columns"] + location_columns
    base_config["columns"] = base_config["columns"] + date_config_fields

    if len(location_columns) > 0:
        base_config["columns"] = base_config["columns"] + [TYPE_COLUMN]

    # save file
    config_filepath = os.path.join(CONFIG_DIR, bigtable_table_name + ".json")
    save_json(config_filepath, base_config)

    return base_config

def combine_subselections(shell_query, subselections):

    subselections = ["({0})".format(ss) for ss in subselections]

    all_subselections = ",\n".join(subselections)

    full_query = shell_query + "\n" + all_subselections
    return full_query


def build_basic_query(selection_fields, group_by_fields, filter_fields):
    query = "SELECT\n"
    query += ",\n".join(selection_fields)
    query += " \nFROM {0}\n"
    if len(filter_fields) > 0:
        query += "WHERE \n" + " AND \n".join(filter_fields)
    if len(group_by_fields) > 0:
        query += "\n GROUP BY \n" + ",\n".join(group_by_fields)

    return query



def create(aggregation_id, aggregation_options, date_id, date_config_fields):
    '''
    '''
    print(get_table_name(aggregation_id, date_id))
    query_filename = get_query_full_filename(get_table_name(aggregation_id, date_id))

    location_levels = {}
    use_locations = False

    # HACK: should use configuration or something
    if "client_loc" in aggregation_id:
        location_levels = LOCATION_LEVELS
        use_locations = True


    # create and save config file
    create_config_file(aggregation_id,
                       aggregation_options,
                       date_id,
                       date_config_fields,
                       location_levels)

    # get baseline selection and group by fields
    (selection_fields, group_by_fields) = get_fields(aggregation_options, date_id, date_config_fields)

    full_query = ""

    if use_locations:
        # build first part of query
        shell_query = get_shell_query(aggregation_options,
                                      date_id,
                                      date_config_fields,
                                      location_levels)
        # HACK
        filter_fields = [ field for field in aggregation_options['keys'] if field['name'] != 'client_location_key']
        filters = get_filters(filter_fields)
        # generate all the subselections
        subselections = get_subselections(selection_fields,
                                          group_by_fields,
                                          filters,
                                          location_levels)

        full_query = combine_subselections(shell_query, subselections)
    else:
        # build query without subselections
        filter_fields = get_filters(aggregation_options['fields'] + aggregation_options['keys'])
        full_query = build_basic_query(selection_fields, group_by_fields, filter_fields)

    save_text(query_filename, sqlparse.format(full_query, strip_whitespace=False, reindent=True, indent_width=1))
    # save_text(query_filename, full_query)

def main():
    '''
    '''
    print("saving configs to: {0}".format(CONFIG_DIR))
    print("saving queries to: {0}".format(BASE_DIR))

    # DATE SUFFIX TABLES
    for aggregation_id, aggregation_options in TABLE_BASES.iteritems():
        for date_id, date_config_fields in DATE_AGGEGATIONS.iteritems():
            create(aggregation_id,
                   aggregation_options,
                   date_id,
                   date_config_fields)

# RUN
if __name__ == "__main__":
    main()
