# Read the file in ./schema/prometl-cpu-util.json and convert it to a json with the following format:
# "name": {
#    "type": "string", mode: "nullable", "description": "The name of the metric"}
#

import json

# Read the file in ./schema/prometl-cpu-util.json as a function
def read_json_file(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data

# Convert the json to a json with the following format:
# "name": {
#    "type": "string", mode: "nullable"}
def convert_json_to_schema(json_data):
    schema = {}
    for item in json_data['array']:
        schema[item['name']] = {
            'type': item['type'],
            'mode': item['mode']
        }
    # print the schema
    print(schema)
    return schema

# Write the json to a file
def write_json_to_file(file_path, json_data):
    with open(file_path, 'w') as f:
        json.dump(json_data, f, indent=4)

# main function
if __name__ == '__main__':
    # Read the file in ./schema/prometl-cpu-util.json
    json_data = read_json_file('./schema/prometl-cpu-util.json')
    # Convert the json to a json with the following format:
    # "name": {
    #    "type": "string", mode: "nullable"}
    schema = convert_json_to_schema(json_data)
    # Write the json to a file
    write_json_to_file('./schema/prometl-cpu-util-schema1.json', schema)