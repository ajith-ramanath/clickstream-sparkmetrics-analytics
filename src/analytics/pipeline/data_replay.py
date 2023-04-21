import boto3
import logging
import pandas as pd
from botocore.exceptions import ClientError
import argparse
import s3fs
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# S3 count number of files in a bucket
def read_s3_bucket(bucket, folder, assumed_role, region):
    os.environ['AWS_DEFAULT_REGION'] = region

    # Create an S3 filesystem object using the assumed role credentials
    s3 = s3fs.S3FileSystem(
            anon=False,
            key=assumed_role['Credentials']['AccessKeyId'],
            secret=assumed_role['Credentials']['SecretAccessKey'],
            token=assumed_role['Credentials']['SessionToken']
        )

    # Specify the S3 path for the Parquet file(s)
    path = bucket + '/' + folder
    s3_path = 's3://' + path

    # List the files in the S3 path
    files = s3.ls(s3_path)
    
    return files

# Function to create an MSK client with the temporary credentials
def create_msk_client(credentials):
    # Create an MSK client with the temporary credentials
    msk_client = boto3.client('kafka',
                            aws_access_key_id=credentials['AccessKeyId'],
                            aws_secret_access_key=credentials['SecretAccessKey'],
                            aws_session_token=credentials['SessionToken'])

    return msk_client


# Function to create a Kafka producer with the MSK client
def create_kafka_producer(msk_client, kafka_cluster_arn):
    # Create a Kafka producer
    kafka_producer = msk_client.create_producer(
        ClusterArn=kafka_cluster_arn, ClientId="data_replay"
    )
    return kafka_producer


# Function to create a Kafka topic with the MSK client
def create_kafka_topic(msk_client, kafka_cluster_arn, kafka_topic):
    # Create a Kafka topic
    kafka_topic = msk_client.create_topic(
        ClusterArn=kafka_cluster_arn,
        TopicName=kafka_topic,
        NumberOfPartitions=4,
        ReplicationFactor=1,
    )
    return kafka_topic


# Read from the S3 folder and return the keys using the temporary credentials
def read_pq_files_and_send_msk(files, kafka_producer, kafka_topic):
    # Use Pandas to read the Parquet file(s) from S3
    for file in files:
        df = pd.read_parquet(file, engine='pyarrow', filesystem='s3://')
        #print(df.head(n=10).to_string(index=False))
        send_data_msk(df, kafka_producer, kafka_topic)


# Function to send data to MSK
def send_data_msk(df, kafka_producer, kafka_topic):
    # Create a list of records
    records = df.to_dict(orient="records")

    # Send the records to the Kafka topic
    response = kafka_producer.send_records(
        Records=records, 
        StreamName=kafka_topic['TopicArn']
    )

    # Print the response
    print(response)


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

    # Arguments for threads
    parser.add_argument("--threads", help="Number of threads", type=int, default=16)

    args = parser.parse_args()
    if args.thread is None or args.thread < 16:
        # Want a min of 16 threads
        args.threads = 16
    
    # If threads is not a multiple of 16, make it a multiple of 16
    if args.threads % 16 != 0:
        args.threads = args.threads + (16 - args.threads % 16)

    # If threads is greater than 256, make it 256
    if args.threads > 256:
        args.threads = 256

    logging.info("Arguments: %s", args)

    try:
        # Assume role
        assumed_role = assume_role(args.role, args.session)
        credentials = assumed_role['Credentials']

        # Create an MSK client with the temporary credentials
        msk_client = create_msk_client(credentials)

        # Create a Kafka producer with the MSK client
        kafka_producer = create_kafka_producer(msk_client, args.kafka_cluster_arn)

        # Create a Kafka topic with the MSK client
        kafka_topic = create_kafka_topic(msk_client, args.kafka_cluster_arn, "data_replay")

        # Read S3 bucket
        all_files = read_s3_bucket(args.bucket, args.folder, assumed_role, args.region)

        files_count = len(all_files)
        files_per_thread = files_count // args.threads

        # Make things concurrent now. Implement thread pool
        futures = []
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            for i in range(args.threads):
                start = i * files_per_thread
                end = start + files_per_thread
                if i == args.threads - 1:
                    end = files_count
                future = executor.submit(read_pq_files_and_send_msk, all_files[start:end], msk_client, kafka_producer, kafka_topic)
                futures.append(future)

    except Exception as e:
        logging.exception("Error in data replay")
        raise

if __name__ == "__main__":
    main()