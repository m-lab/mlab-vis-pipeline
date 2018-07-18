'''
Time table specific configurations
'''

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