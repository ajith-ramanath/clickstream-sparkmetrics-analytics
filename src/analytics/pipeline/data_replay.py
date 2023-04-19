import boto3
import logging
import pandas as pd
from botocore.exceptions import ClientError
import argparse
import json

# Function to assume role and get temporary credentials
def assume_role(role_arn, role_session_name):
    # Create an STS client object that represents a live connection to the
    # STS service.
    sts_client = boto3.client("sts")

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName=role_session_name
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials = assumed_role_object["Credentials"]

    return credentials

# Read from the S3 folder and return the keys using the temporary credentials
def read_s3_folder(bucket, folder, credentials):
    # Create a new S3 client object using the temporary credentials.
    s3_client = boto3.client("s3", aws_access_key_id=credentials["AccessKeyId"], aws_secret_access_key=credentials["SecretAccessKey"], aws_session_token=credentials["SessionToken"])

    # List the objects in the S3 bucket.
    objects = s3_client.list_objects(Bucket=bucket, Prefix=folder)

    # Read the contents of one of the objects.
    object_keys = []
    for obj in objects["Contents"]:
        object_keys.append(obj["Key"])

    return object_keys


# Function that sends data to Kafka
def data_replay(object_keys, kafka_cluster_arn, kafka_partition_key, credentials, bucket):
    # Create a new S3 client object using the temporary credentials.

    s3_client = boto3.client("s3", aws_access_key_id=credentials["AccessKeyId"], aws_secret_access_key=credentials["SecretAccessKey"], aws_session_token=credentials["SessionToken"])

    # Define the dataframe
    df = pd.DataFrame()

    # Loop through the object keys
    for key in object_keys:
        # Read the contents of one of the objects.
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(obj["Body"])

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
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Starting data replay")

    # Arguments parsing
    parser = argparse.ArgumentParser()
    
    # Arguments for bucket and folder
    parser.add_argument("--bucket", help="S3 bucket")
    parser.add_argument("--folder", help="S3 folder")
    
    # Argument - role ARN to assume
    parser.add_argument("--role", help="Role to assume")
    parser.add_argument("--session", help="Session name")

    # Arguments for Kafka
    parser.add_argument("--kafka_cluster_arn", help="Kafka cluster ARN")
    parser.add_argument("--kafka_partition_key", help="Kafka partition key")

    args = parser.parse_args()

    try:
        # Assume role
        credentials = assume_role(args.role, "data_replay")

        # Read S3 folder
        object_keys = read_s3_folder(args.bucket, args.folder, credentials)

        # Replay data to Kafka
        df = data_replay(object_keys, args.kafka_cluster_arn, args.kafka_partition_key, credentials, args.bucket)

        # Send data to Kafka
        send_data_msk(df, args.kafka_cluster_arn, args.kafka_partition_key)

    except Exception as e:
        logging.exception("Error in data replay")
        raise

if __name__ == "__main__":
    main()
