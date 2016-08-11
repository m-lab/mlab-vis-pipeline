AGGREGATIONS = {

    "location_client_asn_number_list" : {
        "table_name" : "location_client_asn_number_list",
        "query_file": "location_client_asn_number_list",
        "json_file" : "location_client_asn_number_list",

        "key_length" : 30,
        "key_name" : "location_id",
        "key_columns" : [
            "location_id",
            "client_asn_number"
        ],

        "key_fields" : [
            {"length": 20, "type": "string", "name": "location_id", "family": "meta"},
            {"length": 10, "type": "string", "name": "client_asn_number", "family": "meta"}
        ],

        "region_key_fields": {
            "city" : ["client_continent_code", "client_country_code", "client_region_code", "client_city"],
            "region" : ["client_continent_code", "client_country_code", "client_region_code"],
            "country" : ["client_continent_code", "client_country_code"],
            "continent": ["client_continent_code"]
        },

        "fields" : [
            {"name" : "client_asn_name", "family" : "meta", "type": "string"},
            {"name": "type", "family": "meta", "type": "string"},
            {"name": "client_city", "family": "meta", "type": "string"},
            {"name": "client_region", "family": "meta", "type": "string"},
            {"name": "client_region_code", "family": "meta", "type": "string"},
            {"name": "client_country", "family": "meta", "type": "string"},
            {"name": "client_country_code", "family": "meta", "type": "string"},
            {"name": "client_continent", "family": "meta", "type": "string"},
            {"name": "client_continent_code", "family": "meta", "type": "string"},
        ],

        "binned_fields" : [
            {"type": "double", "name": "download_speed_mbps", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps", "family": "data"},
        ],

        "timed_fields" : [
            {"type": "integer", "name": "test_count", "family": "meta"},
            {"type": "double", "name": "download_speed_mbps_median", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_median", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_avg", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_avg", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_min", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_min", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_max", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_max", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_stddev", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_stddev", "family": "data"},
        ]
    },

    "location_search" : {

        "table_name" : "location_search",
        "query_file": "location_search",
        "json_file" : "location_search",

        "key_length" : 60,
        "key_name": "location_key",
        "key_columns" : [
            "location",
            "client_region_code",
            "client_country_code",
            "client_continent_code"
        ],
        "key_fields" : [
            {"name" : "location_key", "family" : "meta", "type": "string", "length" : 60}
        ],
        "fields": [
            {"name": "test_count", "family" : "data", "type" : "integer"},
            {"name": "last_three_month_test_count", "family" : "data", "type" : "integer"},
            {"name": "location", "family" : "meta", "type" : "string", "length" : 40},
            {"name": "type", "family" : "meta", "type" : "string", "length" : 40},
            {"name": "client_region", "family" : "meta", "type" : "string", "length" : 40},
            {"name": "client_country", "family" : "meta", "type" : "string", "length" : 40},
            {"name": "client_continent", "family" : "meta", "type" : "string", "length" : 40}
        ]
    },

    "location_list": {
        "table_name": "location_list",
        "query_file" : "location_list",
        "json_file" : "location_list",

        "key_name": "parent_location_id",

        "key_fields" : [
            {"length": 20, "type": "string", "name": "parent_location_id", "family": "meta"},
            {"length": 45, "type": "string", "name": "child_location_name", "family": "meta"}
        ],

        "region_key_fields": {
            "city" : ["client_continent_code", "client_country_code", "client_region_code"],
            "region" : ["client_continent_code", "client_country_code"],
            "country" : ["client_continent_code"],
            "continent": ""
        },

        "fields" : [
            {"length": 10, "type": "string", "name": "type", "family": "meta"},
            {"length": 45, "type": "string", "name": "client_region", "family": "meta"},
            {"length": 2, "type": "string", "name": "client_region_code", "family": "meta"},
            {"length": 40, "type": "string", "name": "client_country", "family": "meta"},
            {"length": 2, "type": "string", "name": "client_country_code", "family": "meta"},
            {"length": 13, "type": "string", "name": "client_continent", "family": "meta"},
            {"length": 2, "type": "string", "name": "client_continent_code", "family": "meta"},
        ],
        "timed_fields" : [
            {"type": "integer", "name": "test_count", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_median", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_median", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_avg", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_avg", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_min", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_min", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_max", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_max", "family": "data"},
            {"type": "double", "name": "download_speed_mbps_stddev", "family": "data"},
            {"type": "double", "name": "upload_speed_mbps_stddev", "family": "data"},
        ]
    }
}

TIME_RANGES = [
    "last_week",
    "last_month",
    "last_year"
]

HISTOGRAM_BINS = [0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76,80,
    84,88,92,96,100]

TEST_DATE_COMPARISONS =  {
    "last_week" : "DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, \"DAY\")",
    "last_month" : "DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, \"MONTH\")",
    "last_year" : "DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, \"YEAR\")"
}

LOCATION_CLIENT_ASN_LEVELS = [
    {
        "type": "city",
        "location_field": "client_city",
        "fields" : ["client_asn_number", "client_asn_name", "client_city", "client_region", "client_country",
            "client_continent", "client_region_code", "client_country_code",
            "client_continent_code"]
    },
    {
        "type": "region",
        "location_field": "client_region",
        "fields" : ["client_asn_number", "client_asn_name", "client_region", "client_country",
            "client_continent", "client_region_code", "client_country_code",
            "client_continent_code"]
    },
    {
        "type": "country",
        "location_field": "client_country",
        "fields" : ["client_asn_number", "client_asn_name", "client_country", "client_continent",
            "client_country_code", "client_continent_code"]
    },
    {
        "type": "continent",
        "location_field": "client_continent",
        "fields" : ["client_asn_number", "client_asn_name", "client_continent", "client_continent_code"]
    }
]

LOCATION_LEVELS = [
    {
        "type": "city",
        "location_field": "client_city",
        "fields" : ["client_city", "client_region", "client_country",
            "client_continent", "client_region_code", "client_country_code",
            "client_continent_code"]
    },
    {
        "type": "region",
        "location_field": "client_region",
        "fields" : ["client_region", "client_country",
            "client_continent", "client_region_code", "client_country_code",
            "client_continent_code"]
    },
    {
        "type": "country",
        "location_field": "client_country",
        "fields" : ["client_country", "client_continent",
            "client_country_code", "client_continent_code"]
    },
    {
        "type": "continent",
        "location_field": "client_continent",
        "fields" : ["client_continent", "client_continent_code"]
    }
]
