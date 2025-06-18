import boto3
import pandas as pd
from io import StringIO
import logging
import json

# SQS setup
sqs = boto3.client('sqs', region_name='us-east-1')  # e.g., 'us-east-1'

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/884203033942/webscraping'

# def send_test_messages():
#     for i in range(10):
#         message_body = f"Hello message {i+1}"
#         response = sqs.send_message(
#             QueueUrl=QUEUE_URL,
#             MessageBody=message_body
#         )
#         print(f"Sent: {message_body} | MessageId: {response['MessageId']}")

def read_from_s3(bucket, key):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(data))
        logging.info(f"Read CSV from s3://{bucket}/{key}")
        return df
    except Exception as e:
        logging.error(f"Failed to read from S3: {e}")
        return None

def send_records_to_sqs(df):
    try:
        for _, row in df.iterrows():
            # Convert row to dictionary and then to JSON string
            message_body = json.dumps(row.to_dict())
            response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=message_body
            )
            print(f"Sent record | MessageId: {response['MessageId']}")
    except Exception as e:
        logging.error(f"Failed to send messages to SQS: {e}")

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Read from S3
    bucket = 'rapidious-datalake'
    key = 'webscraping/dealer_input/config/testing.csv'  # Replace with your actual S3 key

    df = read_from_s3(bucket, key)
    if df is not None:
        send_records_to_sqs(df)