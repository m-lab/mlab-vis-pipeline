#!/usr/bin/env python
'''
Create configs and query files for bigtable tables that have time
as the end parameter.
'''

import json
import os

BIGQUERY_DATE_TABLE = "[mlab-oti:bocoup_prod.all_ip_by_day]"
BIGQUERY_HOUR_TABLE = "[mlab-oti:bocoup_prod.all_ip_by_hour]"

QUERY_BASEDIR = os.path.join("..", "..", "dataflow", "data", "bigtable", "queries")
CONFIG_QUERY_BASEDIR = os.path.join(".", "data", "bigtable", "queries")

# Aggregations that represent keys and query group-bys
AGGREGATIONS = {
    # SERVER ASN NAME x CLIENT ASN NUMBER x CLIENT LOCATION
    # Naming needs to stay under 50 characters
    "server_asn_client_asn_client_city": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_city", "length": 40, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],
    "server_asn_client_asn_client_region": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],
    "server_asn_client_asn_client_country": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],
    "server_asn_client_asn_client_cont": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],

    # SERVER ASN NAME x CLIENT LOCATION
    "server_asn_client_city": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_city", "length": 40, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],
    "server_asn_client_region": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],
    "server_asn_client_country": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],
    "server_asn_client_cont": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],

    # CLIENT ASN NUMBER x CLIENT LOCATION
    "client_asn_client_city": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_city", "length": 40, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
    ],
    "client_asn_client_region": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
    ],
    "client_asn_client_country": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
    ],
    "client_asn_client_cont": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"},
    ],

    # CLIENT LOCATION
    "client_city": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_city", "length": 40, "type": "string", "family": "meta"}
    ],
    "client_region": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_region_code", "length": 2, "type": "string", "family": "meta"}
    ],
    "client_country": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
        {"name": "client_country_code", "length": 2, "type": "string", "family": "meta"}
    ],
    "client_cont": [
        {"name": "client_continent_code", "length": 2, "type": "string", "family": "meta"},
    ],

    # OTHERS
    "client_asn": [
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"}
    ],
    "server_asn": [
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"}
    ],
    "server_asn_client_asn": [
        {"name": "server_asn_name", "length": 40, "type": "string", "family": "meta"},
        {"name": "client_asn_number", "length": 10, "type": "string", "family": "meta"}
    ],

}

ADDITIONAL_FIELDS = {
    # SERVER ASN NAME x CLIENT ASN NUMBER x CLIENT LOCATION
    "server_asn_client_asn_client_city": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],
    "server_asn_client_asn_client_region": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],
    "server_asn_client_asn_client_country": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],
    "server_asn_client_asn_client_cont": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],

    # SERVER ASN NAME x  CLIENT LOCATION
    "server_asn_client_city": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"},
    ],
    "server_asn_client_region": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"},
    ],
    "server_asn_client_country": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
    ],
    "server_asn_client_cont": [
        {"name": "client_continent", "type": "string", "family": "meta"},
    ],

    # CLIENT ASN NUMBER x CLIENT LOCATION
    "client_asn_client_city": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],
    "client_asn_client_region": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],
    "client_asn_client_country": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],
    "client_asn_client_cont": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],

    # CLIENT LOCATION
    "client_city": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"}
    ],
    "client_region": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
        {"name": "client_region", "type": "string", "family": "meta"}
    ],
    "client_country": [
        {"name": "client_continent", "type": "string", "family": "meta"},
        {"name": "client_country", "type": "string", "family": "meta"},
    ],
    "client_cont": [
        {"name": "client_continent", "type": "string", "family": "meta"},
    ],

    # OTHERS
    "client_asn": [
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ],
    "server_asn": [],
    "server_asn_client_asn": [
        {"name": "client_asn_name", "type": "string", "family": "meta"}
    ]
}

AGGREGATION_FILTERS = {
    # SERVER ASN NAME x CLIENT ASN NUMBER x CLIENT LOCATION
    "server_asn_client_asn_client_city": [
        "LENGTH(client_city) > 0",
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0"
    ],
    "server_asn_client_asn_number_client_region": [
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0",
        "LENGTH(server_asn_name) > 0"
    ],
    "server_asn_client_asn_number_client_country": [
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0",
        "LENGTH(server_asn_name) > 0"
    ],
    "server_asn_client_asn_number_client_cont": [
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0",
        "LENGTH(server_asn_name) > 0"
    ],

    # SERVER ASN NAME x CLIENT LOCATION
    "server_asn_client_city": [
        "LENGTH(client_city) > 0",
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
    ],
    "server_asn_client_region": [
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
    ],
    "server_asn_client_country": [
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
    ],
    "server_asn_client_cont": [
        "LENGTH(client_continent_code) > 0",
    ],

    # CLIENT ASN NUMBER x CLIENT LOCATION
    "client_asn_client_city": [
        "LENGTH(client_city) > 0",
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0"
    ],
    "client_asn_client_region": [
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0"
    ],
    "client_asn_client_country": [
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0"
    ],
    "client_asn_client_cont": [
        "LENGTH(client_continent_code) > 0",
        "LENGTH(client_asn_number) > 0",
        "LENGTH(client_asn_name) > 0"
    ],

    # CLIENT LOCATION
    "client_city": [
        "LENGTH(client_city) > 0",
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0",
    ],
    "client_region": [
        "LENGTH(client_region_code) > 0",
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0"
    ],
    "client_country": [
        "LENGTH(client_country_code) > 0",
        "LENGTH(client_continent_code) > 0"
    ],
    "client_cont": [
        "LENGTH(client_continent_code) > 0"
    ],

    # OTHER
    "client_asn": [
        "LENGTH(client_asn_number) > 0"
    ],
    "server_asn": [
        "LENGTH(server_asn_name) > 0"
    ],
    "server_asn_client_asn": [
        "LENGTH(client_asn_number) > 0",
        "LENGTH(server_asn_name) > 0"
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
    "day": ["DATE(test_date) AS date"],
    "month": ["STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), \"%Y-%m\") as date"],
    "year": ["STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), \"%Y\") as date"],
    "day_hour": [
        "DATE(test_date) AS date",
        "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), \"%H\") as hour"
    ],
    "month_hour": [
        "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), \"%Y-%m\") as date",
        "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), \"%H\") as hour"
    ],
    "year_hour": [
        "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), \"%Y\") as date",
        "STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), \"%H\") as hour"
    ]
}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../", "dataflow"))

CONFIG_DIR = os.path.join(BASE_DIR, "data", "bigtable")
QUERY_FOLDER = os.path.abspath(os.path.join(CONFIG_DIR, "queries"))

DATE_CONFIG_TEMPLATE_FILENAME = os.path.abspath(os.path.join(
                                os.path.dirname(os.path.realpath(__file__)),
                                "templates", "config_template.json"))

LIST_CONFIG_TEMPLATE_FILENAME = os.path.abspath(os.path.join(
                                os.path.dirname(os.path.realpath(__file__)),
                                "templates", "config_template.json"))

SQL_TEMPLATE_FILENAME = os.path.abspath(os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "templates", "sql_template.sql"))

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
    if(date_id):
        return "{0}_by_{1}".format(aggregation_id, date_id)
    else:
        return "{0}_list".format(aggregation_id)


def get_queries(aggregation_config_fields,
                date_id,
                date_config_fields,
                additional_config_fields):
    '''
    Output custom portions of the query file.
    aggregation_config_fields : value from LOCATION

    TODO: not sure this should build both
    the group by queries and the select queries.
    though they are interdependent - so maybe.
    '''

    if(not date_id):
        date_config_fields = []

    additional_field_names = [x["name"] for x in additional_config_fields]
    group_by_fields = [x["name"] for x in aggregation_config_fields]
    group_by_fields += [x["name"] for x in date_config_fields]
    group_by_fields += additional_field_names

    group_by_query = ",\n".join(group_by_fields)

    if "date" in group_by_fields:
        group_by_fields.remove("date")
    if "hour" in group_by_fields:
        group_by_fields.remove("hour")

    date_queries = []
    if date_id in DATE_QUERIES:
        date_queries = DATE_QUERIES[date_id]

    group_by_fields = group_by_fields + date_queries
    select_query = ",\n".join(group_by_fields)

    return (select_query, group_by_query)


def get_filter_query(aggregation_id):
    '''
    Return Filter by SQL snippet
    '''
    filter_query = ""

    if (aggregation_id in AGGREGATION_FILTERS):
        filters = AGGREGATION_FILTERS[aggregation_id]
        filter_query = "WHERE " + " AND ".join(filters)
    return filter_query


def create_config_file(aggregation_id,
                       aggregation_config_fields,
                       date_id,
                       date_config_fields,
                       additional_config_fields):
    '''
    Creates bigtable config files.
    '''
    # compute some names
    bigtable_table_name = get_table_name(aggregation_id, date_id)
    query_filename = get_query_relative_filename(bigtable_table_name)

    # get template
    base_config = read_json(DATE_CONFIG_TEMPLATE_FILENAME)

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
    base_config["row_keys"] = aggregation_config_fields + date_config_fields

    # columns
    base_config["columns"] = base_config["columns"] + aggregation_config_fields
    base_config["columns"] = base_config["columns"] + additional_config_fields
    base_config["columns"] = base_config["columns"] + date_config_fields

    # save file
    config_filepath = os.path.join(CONFIG_DIR, bigtable_table_name + ".json")
    save_json(config_filepath, base_config)

    return base_config


def create_query_file(aggregation_id,
                      date_id,
                      select_query,
                      group_by_query,
                      filter_query):
    '''
    Creates bigquery sql file.
    '''
    query_filename = get_query_full_filename(get_table_name(aggregation_id, date_id))
    base_query = read_text(SQL_TEMPLATE_FILENAME)

    base_query += "\n{0}\n\n".format(select_query)

    base_query += "FROM\n  {0}\n"
    base_query += "{0}\n\n".format(filter_query)
    base_query += "GROUP BY\n"
    base_query += "{0}\n\n".format(group_by_query)

    # save to full path
    save_text(query_filename, base_query)


def create(aggregation_id, aggregation_config_fields, date_id, date_config_fields):
    '''
    '''
    print(get_table_name(aggregation_id, date_id))

    additional_config_fields = []
    if(aggregation_id in ADDITIONAL_FIELDS):
        additional_config_fields = ADDITIONAL_FIELDS[aggregation_id]

    # create and save config file
    create_config_file(aggregation_id,
                       aggregation_config_fields,
                       date_id,
                       date_config_fields,
                       additional_config_fields)

    # get out front and back part of query
    (select_query, group_by_query) = get_queries(aggregation_config_fields,
                                                   date_id,
                                                   date_config_fields,
                                                   additional_config_fields)

    filter_query = get_filter_query(aggregation_id)

    # create and save query
    create_query_file(aggregation_id,
                      date_id,
                      select_query,
                      group_by_query,
                      filter_query)


def main():
    '''
    '''
    print("saving configs to: {0}".format(CONFIG_DIR))
    print("saving queries to: {0}".format(BASE_DIR))

    # DATE SUFFIX TABLES
    for aggregation_id, aggregation_config_fields in AGGREGATIONS.iteritems():
        for date_id, date_config_fields in DATE_AGGEGATIONS.iteritems():
            create(aggregation_id,
                   aggregation_config_fields,
                   date_id,
                   date_config_fields)

# RUN
main()
