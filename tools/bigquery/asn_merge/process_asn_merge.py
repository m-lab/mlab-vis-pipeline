"""
Tool to create the CSV used for the asn_merge table in BigQuery.
It reads in asn_merge.json and converts it to CSV format.
"""
import os
import json
import csv

from collections import OrderedDict

CUR_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))

OUTPUT_DIRECTORY = os.path.join(CUR_DIR, "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIRECTORY, "asn_merge.csv")
INPUT_FILE = os.path.join(CUR_DIR, "..", "..", "..", "dataflow", "data", "bigquery", "asn_merge", "asn_merge.json")

if not os.path.exists(OUTPUT_DIRECTORY):
    os.makedirs(OUTPUT_DIRECTORY)

def read_json(filename):
    '''
    read json
    '''
    data = {}
    with open(filename) as data_file:
        data = json.load(data_file)
    return data

def write_csv(filename, data_list):
    """
    Write the CSV file to disk
    """
    with open(filename, 'w') as csvfile:
        header = data_list[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        for row in data_list:
            writer.writerow(row)

def convert_to_rows(entry):
    """
    Expand an entry to multiple rows in the table, appending `x` to the end of the ASN.
    """
    asns = sorted(entry["asns"])
    # master_asn = ','.join(asns)
    master_asn = asns[0] + "x"
    new_name = None

    if "name" in entry:
        new_name = entry["name"]

    rows = []
    for asn in asns:
        row = OrderedDict([("asn_numer",asn), ("new_asn_number", master_asn), ("new_asn_name", new_name)])
        rows.append(row)
    return rows

def main(input_filename, output_filename):
    in_data_list = read_json(input_filename)

    rows = []

    for entry in in_data_list:
        rows += convert_to_rows(entry)

    write_csv(output_filename, rows)


main(INPUT_FILE, OUTPUT_FILE)
