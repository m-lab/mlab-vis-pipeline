#!/usr/bin/env python
'''

'''
import os
import shutil
import glob
import argparse

from tools.utils import read_json, save_json

def main(config_dir, tablename):
    '''
    Main
    '''

    premade_templates_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "templates", "premade")

    json_files = glob.glob(os.path.join(premade_templates_dir, '*.json'))
    print "{0} json files".format(len(json_files))

    for json_file in json_files:
        file_name = os.path.basename(json_file)
        contents = read_json(json_file)
        contents['bigquery_table'] = tablename
        config_filepath = os.path.join(config_dir, file_name)
        save_json(config_filepath, contents)

    sql_files = glob.glob(os.path.join(premade_templates_dir, '*.sql'))
    print "{0} sql files".format(len(sql_files))

    query_dir = os.path.abspath(os.path.join(config_dir, "queries"))

    for sql_file in sql_files:
        print sql_file
        shutil.copy(sql_file, query_dir)

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    PARSER.add_argument('--configs',
                        help='Directory of Bigtable config files')

    PARSER.add_argument('--project',
                        help='Which M-Lab project to deploy to')

    ARGS = PARSER.parse_args()
    if not ARGS.configs:
        print '--configs required. Provide config file directory'
    elif not ARGS.project:
        print '--project required. Provide project name'
    else:
        # TABLENAME = "[%s:data_viz.all_ip_by_day]" % ARGS.project
        TABLENAME = "data_viz.all_ip_by_day"
        main(ARGS.configs, TABLENAME)
