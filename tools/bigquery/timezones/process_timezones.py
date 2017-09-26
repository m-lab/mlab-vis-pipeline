#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
A tool for processing time zone data that can then be used to
convert a time and lat/lng coordinates to a local time.
"""
from __future__ import print_function

import os
import csv

CUR_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))
TIMEZONE_DIR = os.path.join(CUR_DIR, "..", "..", "..", "dataflow", "data", "bigquery", "timezonedb")
ZONES_FILE = os.path.join(TIMEZONE_DIR, "zone.csv")
TIMEZONES_FILE = os.path.join(TIMEZONE_DIR, "timezone.csv")
MERGED_TIMEZONE_FILE = os.path.join(TIMEZONE_DIR, "merged_timezone.csv")

def build_zone_map():
    '''
    Build zone map
    '''
    with open(ZONES_FILE, 'rb') as csvfile:
        fieldnames = ["zone_id", "country_name", "zone_name"]
        reader = csv.DictReader(csvfile, fieldnames=fieldnames)

        zone_dict = {}

        # zone id, country code, zone name
        for row in reader:
            zone_dict[row["zone_id"]] = row["zone_name"]

    return zone_dict

def build_timezone_map():
    '''
    Build zone map
    '''
    zone_dict = build_zone_map()
    fieldnames = ["zone_id", "timezone_name", "timestart",
                  "gmt_offset_seconds", "dst_flag"]
    with open(TIMEZONES_FILE, 'rb') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=fieldnames)

        rows = []
        for row in reader:
            row["zone_id"] = zone_dict[row["zone_id"]]
            rows.append(row)

        # Write the new CSV file with new columns
        with open(MERGED_TIMEZONE_FILE, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    print("Timezones CSV generated at {0}".format(MERGED_TIMEZONE_FILE))

def main():
    """
    The main program starting point, processes the CSV.
    """
    build_timezone_map()

if __name__ == "__main__":
    main()
