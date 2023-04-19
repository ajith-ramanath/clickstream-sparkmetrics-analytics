# Script to generate dummy datasets for click stream use-case for a given schema

import json
import random
import time
import datetime
import os
import sys
import argparse
import logging
import logging.config

# create a function to read the schema file in ./schema/clickstreamevent.json
def read_schema(schema_file):
    try:
        schema = json.loads(open(schema_file).read())
    except ValueError as e:
        logging.error("Error parsing json string: %s", e)
        return None
    return schema

 #create a function that randomly generates some Android phone models
def generate_random_phone_manufacturer_list():
    phone_models = ['Oppo Reno', 'Motorola Moto G', 'Sony Xperia', 'LG G series', 'HTC One', 'Asus Zenfone', 'OnePlus', 'Nokia Lumia', 'Huawei Honor']
    return random.choice(phone_models)

def main():
    # create a parser to parse command line arguments
    parser = argparse.ArgumentParser(description='Generate random data for the analytics pipeline')
    parser.add_argument('-s', '--schema', required=True, help='The schema to use for generating the data')
    parser.add_argument('-n', '--num', required=True, help='The number of data points to generate')
    parser.add_argument('-o', '--output', required=True, help='The output file to write the data to')
    args = parser.parse_args()

    # read the schema file
    schema = read_schema(args.schema)
    # check if the schema is valid
    if schema is None:
        logging.error("Error: %s is not a valid schema", args.schema)
        sys.exit(1)

    # create a list to store the data
    data = []

    # generate the data
    for i in range(int(args.num)):
        data.append(generate_random_phone_manufacturer_list(schema))


# main function
if __name__ == '__main__':
    main()

