#!/usr/bin/env python
'''

'''
import os
import shutil
import glob

from tools.bigtable.create_bigtable_time_configs import \
    QUERY_DIR, CONFIG_DIR

PREMADE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           "templates", "premade")

def main():
    '''
    Main
    '''
    json_files = glob.glob(os.path.join(PREMADE_DIR, '*.json'))
    print "{0} json files".format(len(json_files))

    for json_file in json_files:
        print json_file
        shutil.copy(json_file, CONFIG_DIR)

    sql_files = glob.glob(os.path.join(PREMADE_DIR, '*.sql'))
    print "{0} sql files".format(len(sql_files))

    for sql_file in sql_files:
        print sql_file
        shutil.copy(sql_file, QUERY_DIR)

if __name__ == "__main__":
    main()
