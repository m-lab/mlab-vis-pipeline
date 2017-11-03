"""
Tool to create the CSV used for the location_cleaning table in BigQuery.
It reads in location_cleaning.json and converts it to CSV format.
"""
import os
import json
import csv
import re

CUR_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))

OUTPUT_DIRECTORY = os.path.join(CUR_DIR, "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIRECTORY, "location_cleaning.csv")
INPUT_FILE = os.path.join(CUR_DIR, "data", "location_cleaning.json")

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

def process_row(entry):
    """
    Each entry is currently a row, so just generate the location key and output
    the relevant columns.
    """
    location_key = ''.join((entry["continent_code"],
                            entry["country_code"],
                            entry["region_code"],
                            entry["city"]))

    # normalize the key
    location_key = re.sub(r'[\W|_]', '', location_key.lower())

    return {
        'location_key': location_key,
        'new_city': entry["new_city"],
        'new_region_code': entry["new_region_code"]
    }

def main(input_filename, output_filename):
    """
    Main function that reads the JSON and outputs the CSV
    """

    in_data_list = read_json(input_filename)

    rows = []

    for entry in in_data_list:
        rows.append(process_row(entry))

    write_csv(output_filename, rows)


main(INPUT_FILE, OUTPUT_FILE)
