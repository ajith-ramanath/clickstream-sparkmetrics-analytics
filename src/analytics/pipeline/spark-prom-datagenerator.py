# Script to generate data for the analytics pipeline
# Path: src/analytics/pipeline/datagenerator.py

import json
import random
import time
import datetime
import os
import sys
import argparse
import logging
import logging.config


# create a function that parses json strings and converts them to a dictionary
def parse_json(json_string):
    try:
        json_dict = json.loads(json_string)
    except ValueError as e:
        logging.error("Error parsing json string: %s", e)
        return None
    return json_dict

# create a function that generates a dictionary of random data for the given schema in ./schema/*.json
def generate_random_data(schema):
    data = {}

    for key, value in schema.items():
        if value['type'] == 'string':
            data[key] = str(random.randint(0, 100))
        elif value['type'] == 'INT64':
            data[key] = random.randint(0, 100)
        elif value['type'] == 'FLOAT64':
            data[key] = random.uniform(0, 100)
        elif value['type'] == 'boolean':
            data[key] = bool(random.getrandbits(1))
        elif value['type'] == 'timestamp':
            data[key] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif value['type'] == 'date':
            data[key] = datetime.datetime.now().strftime('%Y-%m-%d')
        else:
            logging.error("Error: %s is not a valid type", value['type'])
            return None
    return data

# main function
if __name__ == '__main__':
    # create a parser to parse command line arguments
    parser = argparse.ArgumentParser(description='Generate random data for the analytics pipeline')
    parser.add_argument('-s', '--schema', required=True, help='The schema to use for generating the data')
    parser.add_argument('-n', '--num', required=True, help='The number of data points to generate')
    parser.add_argument('-o', '--output', required=True, help='The output file to write the data to')
    args = parser.parse_args()

    # read the schema file
    schema = parse_json(open(args.schema).read())
    # check if the schema is valid
    if schema is None:
        logging.error("Error: %s is not a valid schema", args.schema)
        sys.exit(1)

    # create a list to store the data
    data = []

    # generate the data
    for i in range(int(args.num)):
        data.append(generate_random_data(schema))

    # write the data to the output file
    with open(args.output, 'w') as f:
        json.dump(data, f, indent=4)

    logging.info("Successfully generated %s data points", args.num)