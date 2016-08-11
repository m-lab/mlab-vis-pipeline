#!/usr/bin/env python
"""
Tool for creating the data for the maxmind_asn table in BigQuery based on
MaxMind GeoIPASNum input files.

The ASN_NAME_MAP_FILENAME is used to override the names provided from MaxMind.

Usage:

format_maxmind_csv GeoIPASNum2.csv GeoIPASNum2v6.csv asn_name_map.csv
"""
from __future__ import print_function

import sys
import os
import csv
import base64

from ipaddr import IPAddress, v4_int_to_packed

IPV4_FILENAME = sys.argv[1]
IPV6_FILENAME = sys.argv[2]
ASN_NAME_MAP_FILENAME = sys.argv[3]

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIRECTORY = CUR_DIR + "/output"
OUTPUT_FILENAME = OUTPUT_DIRECTORY + '/maxmind_asn.csv'

IP_FAMILY_IPV4 = 2
IP_FAMILY_IPV6 = 10

ASN_STRING_COLUMN = 'asn_string'
ASN_NUMBER_COLUMN = 'asn_number'
ASN_NAME_COLUMN = 'asn_name'
MIN_IP_INT_COLUMN = 'min_ip_int'
MAX_IP_INT_COLUMN = 'max_ip_int'
MIN_IP_HEX_COLUMN = 'min_ip_hex'
MAX_IP_HEX_COLUMN = 'max_ip_hex'
MIN_IP_COLUMN = 'min_ip'
MAX_IP_COLUMN = 'max_ip'
IP_FAMILY_COLUMN = 'ip_family'

OUTPUT_FIELDNAMES = [ASN_STRING_COLUMN,
                     ASN_NUMBER_COLUMN,
                     ASN_NAME_COLUMN,
                     MIN_IP_HEX_COLUMN,
                     MAX_IP_HEX_COLUMN,
                     MIN_IP_COLUMN,
                     MAX_IP_COLUMN,
                     IP_FAMILY_COLUMN]



def init_asn_name_map(filename):
    """
    initializes the map for overriding ASN names provided by MaxMind
    """
    asn_name_map = {}

    with open(filename, 'rUb') as csvfile:
        reader = csv.DictReader(csvfile,
                                fieldnames=[ASN_NUMBER_COLUMN, ASN_NAME_COLUMN])
        # create a map from AS number to AS name
        for row in reader:
            if row[ASN_NAME_COLUMN]:
                asn_name_map[row[ASN_NUMBER_COLUMN]] = row[ASN_NAME_COLUMN]


    return asn_name_map


def get_asn_number_name(asn_string):
    """
    pull out asn number and name return as tuple
    """
    asn_fields = asn_string.split(' ')
    asn_number = asn_fields[0]
    asn_name = ' '.join(asn_fields[1:])
    return (asn_number, asn_name)


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

def process_ipv4_csv(filename, asn_name_map):
    """
    process csv for IPv4 input file
    """

    ip_family = IP_FAMILY_IPV4
    csv_rows = []
    input_fieldnames = [MIN_IP_INT_COLUMN, MAX_IP_INT_COLUMN, ASN_STRING_COLUMN]

    with open(filename, 'rb') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=input_fieldnames)

        for row in reader:
            min_ip = IPAddress(v4_int_to_packed(int(row[MIN_IP_INT_COLUMN])))
            max_ip = IPAddress(v4_int_to_packed(int(row[MAX_IP_INT_COLUMN])))

            (asn_number, asn_name) = get_asn_number_name(row[ASN_STRING_COLUMN])
            row[ASN_NUMBER_COLUMN] = asn_number
            row[ASN_NAME_COLUMN] = asn_name_map[asn_number] if asn_number in asn_name_map else asn_name
            row[MIN_IP_HEX_COLUMN] = hex_encode_ip(min_ip)
            row[MAX_IP_HEX_COLUMN] = hex_encode_ip(max_ip)
            row[MIN_IP_COLUMN] = str(min_ip)
            row[MAX_IP_COLUMN] = str(max_ip)
            row[IP_FAMILY_COLUMN] = ip_family
            del row[MIN_IP_INT_COLUMN]
            del row[MAX_IP_INT_COLUMN]

            csv_rows.append(row)

    # Write the new CSV file with new columns
    with open(OUTPUT_FILENAME, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=OUTPUT_FIELDNAMES)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)

def process_ipv6_csv(filename, asn_name_map):
    """
    process csv for IPv6 input file
    """

    ip_family = IP_FAMILY_IPV6
    csv_rows = []
    input_fieldnames = [ASN_STRING_COLUMN, MIN_IP_COLUMN, MAX_IP_COLUMN,
                        'unused']

    with open(filename, 'rb') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=input_fieldnames)

        for row in reader:
            min_ip = IPAddress(row[MIN_IP_COLUMN])
            max_ip = IPAddress(row[MAX_IP_COLUMN])

            (asn_number, asn_name) = get_asn_number_name(row[ASN_STRING_COLUMN])
            row[ASN_NUMBER_COLUMN] = asn_number
            row[ASN_NAME_COLUMN] = asn_name_map[asn_number] if asn_number in asn_name_map else asn_name
            row[MIN_IP_HEX_COLUMN] = hex_encode_ip(min_ip)
            row[MAX_IP_HEX_COLUMN] = hex_encode_ip(max_ip)
            row[MIN_IP_COLUMN] = str(min_ip)
            row[MAX_IP_COLUMN] = str(max_ip)
            row[IP_FAMILY_COLUMN] = ip_family
            del row['unused']

            csv_rows.append(row)

    # Append the CSV file with new rows for IPv6 (do not write a new csv header)
    with open(OUTPUT_FILENAME, 'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=OUTPUT_FIELDNAMES)
        for row in csv_rows:
            writer.writerow(row)


def main():
    """
    The main program starting point, processes the CSV.
    """
    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)

    asn_name_map = init_asn_name_map(ASN_NAME_MAP_FILENAME)
    process_ipv4_csv(IPV4_FILENAME, asn_name_map)
    process_ipv6_csv(IPV6_FILENAME, asn_name_map)


if __name__ == "__main__":
    main()
