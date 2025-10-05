import os
import subprocess
import json
import time
import boto3
# Import specific types for conditions in DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#boto3.dynamodb.conditions.Key
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from configparser import ConfigParser

# Get configuration
config = ConfigParser()
config.read('ann_config.ini')

# AWS configuration from config file
REGION = config.get('aws', 'AwsRegionName')
S3_INPUT_BUCKET = config.get('s3', 'InputsBucket')
S3_RESULTS_BUCKET = config.get('s3', 'ResultsBucket')
DYNAMO_TABLE = config.get('dynamodb', 'AnnotationsTable')
SQS_QUEUE_NAME = config.get('sqs', 'JobRequestsQueue')
WAIT_TIME_SECONDS = config.getint('sqs', 'WaitTimeSeconds')
MAX_NUMBER_OF_MESSAGES = config.getint('sqs', 'MaxNumberOfMessages')

# Initialize AWS services
s3 = boto3.client('s3', region_name=REGION)
dynamo = boto3.resource('dynamodb', region_name=REGION)
# Initialize SQS client: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
sqs = boto3.client('sqs', region_name=REGION)
table = dynamo.Table(DYNAMO_TABLE)

# Get the queue URL by name: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.get_queue_url
def get_queue_url(queue_name):
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']
    except Exception as e:
        print(f"Error finding SQS queue: {str(e)}")
        return None

def process_message(message, queue_url):
    """Process a message from the SQS queue."""
    try:
        # SQS messages may contain nested JSON in the 'Message' field if coming from SNS
        # JSON parsing: https://docs.python.org/3/library/json.html#json.loads
        body = json.loads(message['Body'])
        
        # Check if message came from SNS (will have a 'Message' field)
        if 'Message' in body:
            # If from SNS, the actual job data is in the 'Message' field as a string
            job_data = json.loads(body['Message'])
        else:
            # Direct SQS message
            job_data = body
        
        # Extract job parameters
        job_id = job_data.get('job_id')
        user_id = job_data.get('user_id')
        bucket = job_data.get('s3_inputs_bucket', S3_INPUT_BUCKET)
        key = job_data.get('s3_key_input_file')
        
        if not job_id or not key:
            print(f"Error: Missing job_id or key parameter in message: {body}")
            return False
            
        # Get the input file S3 object and copy it to a local file
        # Path manipulation: https://docs.python.org/3/library/os.path.html
        directory_prefix = os.path.dirname(key)          # e.g. "peilij/user1234"
        file_part = os.path.basename(key)                # "UUID~file.vcf"
        orig_filename = file_part.split('~', 1)[1]       # "file.vcf"
        
        # Local job directory: jobs/user_id/job_id
        job_dir = os.path.join('jobs', user_id, job_id)
        # Create directory with exist_ok parameter: https://docs.python.org/3/library/os.html#os.makedirs
        os.makedirs(job_dir, exist_ok=True)
        local_input_path = os.path.join(job_dir, orig_filename)
        
        try:
            # Download file from S3: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
            s3.download_file(bucket, key, local_input_path)
        except boto3.exceptions.Boto3Error as e:
            print(f"Error downloading file from S3: {str(e)}")
            return False
        
        # Update job status to RUNNING, only if it's currently PENDING
        # Conditional updates in DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
        try:
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET job_status = :running',
                ConditionExpression=Attr('job_status').eq('PENDING'),
                ExpressionAttributeValues={':running': 'RUNNING'}
            )
        except ClientError as e:
            # Handle conditional update failure: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html#catching-exceptions
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"Job {job_id} is not in PENDING state, skipping")
                return True  # other nodes may be processing it
            else:
                print(f"Database error updating job status: {str(e)}")
                return False
            
        # Launch annotation job as a background process
        # Subprocess for running external commands: https://docs.python.org/3/library/subprocess.html#subprocess.Popen
        try:
            ann_process = subprocess.Popen([
                'python', 'run.py',
                directory_prefix,
                job_id,
                local_input_path
            ])
            
            print(f"Started annotation process for job {job_id}")
            return True
            
        except subprocess.SubprocessError as e:
            print(f"Error starting annotation process: {str(e)}")
            return False
            
    except json.JSONDecodeError as e:
        # JSON parsing errors: https://docs.python.org/3/library/json.html#json.JSONDecodeError
        print(f"Error parsing message JSON: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error processing message: {str(e)}")
        return False

def main():
    """Main function that polls the SQS queue for messages."""
    print(f"Starting SQS poller for queue: {SQS_QUEUE_NAME}")
    
    # Get the queue URL at startup
    queue_url = get_queue_url(SQS_QUEUE_NAME)
    if not queue_url:
        print(f"Could not find SQS queue: {SQS_QUEUE_NAME}")
        print("Will retry every 10 seconds...")
    
    while True:
        try:
            # If we don't have a queue URL yet, try to get it
            if not queue_url:
                queue_url = get_queue_url(SQS_QUEUE_NAME)
                if not queue_url:
                    print(f"Still could not find SQS queue: {SQS_QUEUE_NAME}")
                    time.sleep(10)  # Wait before trying again
                    continue
                else:
                    print(f"Found SQS queue: {queue_url}")
            
            # Use long polling to wait for messages: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
                WaitTimeSeconds=WAIT_TIME_SECONDS,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            # Check if messages were received
            if 'Messages' in response:
                for message in response['Messages']:
                    receipt_handle = message['MessageId']
                    print(f"Processing message: {message['MessageId']}")
                    
                    # Process the message
                    success = process_message(message, queue_url)
                    
                    if success:
                        try:
                            # Delete the message from the queue after successful processing
                            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
                            sqs.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            print(f"Deleted message: {message['MessageId']}")
                        except boto3.exceptions.Boto3Error as e:
                            print(f"Error deleting message: {str(e)}")
            else:
                # No messages, but this is expected with long polling
                pass
                
        except boto3.exceptions.Boto3Error as e:
            print(f"AWS error polling queue: {str(e)}")
            # Wait a bit before trying again
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            # Wait a bit before trying again
            time.sleep(5)

if __name__ == '__main__':
    main()