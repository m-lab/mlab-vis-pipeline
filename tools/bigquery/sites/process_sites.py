#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
A tool for adding IP ranges as ints to the MLab Sites csv, exported from:
https://docs.google.com/spreadsheets/d/16h41_3Nyyxt696FKWIzsj5sev2dCsMb7w9-DYpSoz-g/edit
"""
from __future__ import print_function

import os
import csv
import base64
from ipaddr import IPNetwork

OUTPUT_DIRECTORY = "./output"
OUTPUT_FILE = "{0}/mlab_sites_processed.csv".format(OUTPUT_DIRECTORY)
INPUT_FILE = "../../../dataflow/data/bigquery/mlab-sites/M-Lab Sites - Sites.csv"
IP_MIN_COLUMN = "Machine IPv4 Min IP"
IP_MAX_COLUMN = "Machine IPv4 Max IP"
IP_NETMASK_COLUMN = "Machine IPv4 IP prefix netmask"
IP_NETWORK_ADDR_COLUMN = "Machine IPv4 prefix address"
IP_PREFIX_COLUMN = "Machine IPv4 prefix"
IPV6_PREFIX_COLUMN = "Machine IPv6 prefix"
TRANSIT_PROVIDER = "Transit provider"
SITE_COLUMN = "Site"
DATE_COLUMNS = ["In service", "Retired"]

CONTINENT_CODE_COLUMN = "Continent Code"
CONTINENT_COLUMN = "Region"
CONTINTENT_CODES = {
    "Europe": "EU",
    "Asia": "AS",
    "Africa": "AF",
    "North America": "NA",
    "Antarctica": "AN",
    "South America": "SA",
    "Oceania": "OC"
}

def normalize_ip(ip_string):
    """
    Gets rid of spaces and only use the first chunk in the str
    by splitting at the first space. Also lowercases the string.

    Args:
        ip_string (str): The IP to parse

    Returns:
        normalized ip string
    """
    normalized = ip_string.strip().lower()
    normalized = normalized[:normalized.find(' ')] \
                 if ' ' in normalized else normalized

    return normalized

def get_ip_extent(ip_string):
    """
    Extracts the list of IPs as integers for the ip_string range

    Args:
        ip_string (str): The IP to parse, e.g. "196.49.14.192/26"

    Returns:
        ip_extent (list): The first and last IPs in the network,
        to get as integers, cast as int()
            e.g., (3291549376, 3291549439)
    """
    if not ip_string:
        return None

    # get rid of spaces and only use the first chunk in the str
    normalized = normalize_ip(ip_string)
    ip_network = IPNetwork(normalized)


    ip_extent = (ip_network.network, ip_network.broadcast)

    return ip_extent

def hex_encode_ip(ip_addr):
    """
    Encodes IP packed bytes as hex. We want to encode the packed bytes as hex
    instead of base64 since hex preserves sort order as a string and base64
    does not.

    Args:
        ip_addr (IPAddress): the IP address

    Returns:
        string: hex encoding
    """
    if not ip_addr:
        return None

    return base64.b16encode(ip_addr.packed)

def process_row(row):
    """
    Processes a row in the CSV

    Args:
        row (dict): A row of data

    Returns:
        dict: The row to be written to the CSV
    """

    # Add in IP columns
    ips = get_ip_extent(row[IP_PREFIX_COLUMN])
    ipv6_ips = get_ip_extent(row[IPV6_PREFIX_COLUMN])


    # simplify
    row = {
        'site': row[SITE_COLUMN],
        'min_ip_hex': hex_encode_ip(ips[0]) if ips else None,
        'max_ip_hex': hex_encode_ip(ips[-1]) if ips else None,
        'transit_provider': row[TRANSIT_PROVIDER],
        'min_ip': str(ips[0]) if ips else None,
        'max_ip': str(ips[-1]) if ips else None,
        'ip_prefix': normalize_ip(row[IP_PREFIX_COLUMN]),
        'min_ipv6_hex': hex_encode_ip(ipv6_ips[0]) if ipv6_ips else None,
        'max_ipv6_hex': hex_encode_ip(ipv6_ips[-1]) if ipv6_ips else None,
        'min_ipv6': str(ipv6_ips[0]) if ipv6_ips else None,
        'max_ipv6': str(ipv6_ips[-1]) if ipv6_ips else None,
        'ipv6_prefix': normalize_ip(row[IPV6_PREFIX_COLUMN]),
    }

    return row

def process_csv():
    """
    Adds in the IP related columns to the CSV,
    format the date columns and write the changes to
    a new CSV file.

    Returns:
        (void)
    """
    csv_rows = []
    fieldnames = ['site', 'min_ip_hex', 'max_ip_hex', 'transit_provider',
                  'min_ip', 'max_ip', 'ip_prefix', 'min_ipv6_hex',
                  'max_ipv6_hex', 'min_ipv6', 'max_ipv6', 'ipv6_prefix']

    # Read in the CSV file and augment the columns
    with open(INPUT_FILE, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            csv_rows.append(process_row(row))

    # Write the new CSV file with new columns
    with open(OUTPUT_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)

    print("MLab Sites CSV generated at {0}".format(OUTPUT_FILE))


def main():
    """
    The main program starting point, processes the CSV.
    """

    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)

    process_csv()


if __name__ == "__main__":
    main()
