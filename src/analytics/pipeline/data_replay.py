import boto3
import logging
import pandas as pd
from botocore.exceptions import ClientError
import argparse
import json
import s3fs
import os

# Function to assume role and get temporary credentials
def assume_role(role_arn, role_session_name):

    logging.debug("Assuming role: %s", role_arn)
    logging.debug("Role session name: %s", role_session_name)

    # Create an STS client object that represents a live connection to the
    # STS service.
    sts_client = boto3.client("sts")

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName=role_session_name
    )

    # Return the assumed role 
    return assumed_role_object

# Read from the S3 folder and return the keys using the temporary credentials
def read_s3_and_send_msk(bucket, folder, assumed_role, region, kafka_cluster_arn, kafka_partition_key):

    os.environ['AWS_DEFAULT_REGION'] = region

    # Create an S3 filesystem object using the assumed role credentials
    s3 = s3fs.S3FileSystem(
            anon=False,
            key=assumed_role['Credentials']['AccessKeyId'],
            secret=assumed_role['Credentials']['SecretAccessKey'],
            token=assumed_role['Credentials']['SessionToken'],
        )

    # Specify the S3 path for the Parquet file(s)
    path = bucket + '/' + folder
    s3_path = 's3://' + path

    # List the files in the S3 path
    files = s3.ls(s3_path)

    for file in files:
        # Use Pandas to read the Parquet file(s) from S3
        df = pd.read_parquet(path + file, engine='pyarrow', filesystem='s3://')
        df.head()
        #send_data_msk(df, kafka_cluster_arn, kafka_partition_key)

    # Return the dataframe
    return df

# Function that sends pandas dataframe to Kafka
def send_data_msk(df, kafka_cluster_arn, kafka_partition_key):
    # Create a new MSK client object using the temporary credentials.
    msk_client = boto3.client("kafka")

    # Create a Kafka producer
    kafka_producer = msk_client.create_producer(
        ClusterArn=kafka_cluster_arn, ClientId="data_replay"
    )

    # Create a Kafka topic
    kafka_topic = msk_client.create_topic(
        ClusterArn=kafka_cluster_arn,
        Name="data_replay",
        Partitions=1,
        ReplicationFactor=1,
    )

    # Loop through the dataframe
    for index, row in df.iterrows():
        # Send data to Kafka
        msk_client.send_command(
            ClusterArn=kafka_cluster_arn,
            KafkaBrokerNodeId=kafka_producer["KafkaBrokerNodeId"],
            KafkaBrokerPort=kafka_producer["KafkaBrokerPort"],
            KafkaTopic=kafka_topic["Name"],
            KafkaPartitionKey=kafka_partition_key,
            KafkaPartitionId=0,
            KafkaData=json.dumps(row.to_dict()),
        )

# main function
def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting data replay")

    # Arguments parsing
    parser = argparse.ArgumentParser()
    
    # Arguments for bucket and folder
    parser.add_argument("--bucket", help="S3 bucket")
    parser.add_argument("--folder", help="S3 folder")
    
    # Argument - role ARN to assume
    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--role", help="Role to assume")
    parser.add_argument("--session", help="Session name")

    # Arguments for Kafka
    parser.add_argument("--kafka_cluster_arn", help="Kafka cluster ARN")
    parser.add_argument("--kafka_partition_key", help="Kafka partition key")

    args = parser.parse_args()

    try:
        # Assume role
        assumed_role = assume_role(args.role, args.session)

        # Read S3 folder
        df = read_s3_and_send_msk(args.bucket, args.folder, assumed_role, args.region, args.kafka_cluster_arn, args.kafka_partition_key)

        # Send data to Kafka
        # send_data_msk(df, args.kafka_cluster_arn, args.kafka_partition_key)

    except Exception as e:
        logging.exception("Error in data replay")
        raise

if __name__ == "__main__":
    main()