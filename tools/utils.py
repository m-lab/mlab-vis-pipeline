'''
General utilities shared between bigtable and bigquery tools
'''

import json
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
