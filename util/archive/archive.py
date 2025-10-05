# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import time
import json
import boto3
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# AWS configuration
REGION = config.get('aws', 'AwsRegionName')
DYNAMO_TABLE = config.get('dynamodb', 'AnnotationsTable')
S3_RESULTS_BUCKET = config.get('s3', 'ResultsBucket')
GLACIER_VAULT = config.get('glacier', 'VaultName')
SQS_ARCHIVE_QUEUE = config.get('sqs', 'ArchiveQueue')
SQS_WAIT_TIME = config.getint('sqs', 'WaitTimeSeconds')
SQS_MAX_MESSAGES = config.getint('sqs', 'MaxNumberOfMessages')
ARCHIVE_DELAY_SECONDS = config.getint('archive', 'DelaySeconds')  # 300 seconds = 5 minutes

# Initialize AWS services
s3 = boto3.client('s3', region_name=REGION)
glacier = boto3.client('glacier', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(DYNAMO_TABLE)
sqs = boto3.client('sqs', region_name=REGION)

def get_queue_url(queue_name):
    """Get SQS queue URL by name"""
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']
    except ClientError as e:
        print(f"Error getting queue URL: {str(e)}")
        return None

def check_user_role(user_id):
    """Check if user is free or premium"""
    try:
        # Get user profile using helper function
        # Reference: helpers.get_user_profile - provided utility function
        profile = helpers.get_user_profile(id=user_id)
        # The profile is a dict/DictRow, so we access role as a key
        return profile['role'] if profile and 'role' in profile else 'free_user'
    except Exception as e:
        print(f"Error getting user profile for {user_id}: {str(e)}")
        # Default to free_user if we can't determine role
        return 'free_user'

def archive_to_glacier(s3_bucket, s3_key, vault_name):
    """Archive a file from S3 to Glacier"""
    try:
        # Get the file from S3
        # Reference: S3 get_object - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        print(f"Downloading file from S3: {s3_bucket}/{s3_key}")
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        file_data = response['Body'].read()
        
        # Upload to Glacier
        # Reference: Glacier upload_archive - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
        print(f"Uploading to Glacier vault: {vault_name}")
        glacier_response = glacier.upload_archive(
            vaultName=vault_name,
            body=file_data,
            archiveDescription=f"Results file: {s3_key}"
        )
        
        archive_id = glacier_response['archiveId']
        print(f"Successfully archived to Glacier with ID: {archive_id}")
        
        # Delete from S3 after successful archive
        # Reference: S3 delete_object - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
        s3.delete_object(Bucket=s3_bucket, Key=s3_key)
        print(f"Deleted file from S3: {s3_bucket}/{s3_key}")
        
        return archive_id
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"File not found in S3: {s3_bucket}/{s3_key}")
        elif error_code == 'AccessDenied':
            print(f"Access denied to S3 object: {s3_bucket}/{s3_key}")
        else:
            print(f"AWS error archiving file: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error archiving file: {str(e)}")
        raise

def process_archive_message(message, queue_url):
    """Process an archive message from SQS"""
    try:
        # Parse message body
        body = json.loads(message['Body'])
        
        # Check if message came from SNS
        if 'Message' in body:
            job_data = json.loads(body['Message'])
        else:
            job_data = body
        
        job_id = job_data.get('job_id')
        user_id = job_data.get('user_id')
        complete_time = job_data.get('complete_time')
        
        if not all([job_id, user_id, complete_time]):
            print(f"Missing required fields in message: {job_data}")
            return False
        
        # Calculate time elapsed since job completion
        current_time = int(time.time())
        elapsed_time = current_time - complete_time
        
        # Check if enough time has passed (5 minutes)
        if elapsed_time < ARCHIVE_DELAY_SECONDS:
            # Not ready to archive yet, requeue the message
            # Reference: SQS send_message - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message
            delay_seconds = ARCHIVE_DELAY_SECONDS - elapsed_time
            print(f"Job {job_id} not ready for archival, requeueing with {delay_seconds}s delay")
            
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(job_data),
                DelaySeconds=min(delay_seconds, 900)  # Max delay is 900 seconds
            )
            return True
        
        # Check current user role (not role at submission time)
        # Reference: User role checking - custom function using helpers
        user_role = check_user_role(user_id)
        
        if user_role == 'premium_user':
            print(f"User {user_id} is premium, skipping archive for job {job_id}")
            return True
        
        # Get job details from DynamoDB
        response = table.get_item(Key={'job_id': job_id})
        
        if 'Item' not in response:
            print(f"Job {job_id} not found in database")
            return False
        
        job = response['Item']
        
        # Check if already archived
        if 'results_file_archive_id' in job:
            print(f"Job {job_id} already archived")
            return True
        
        # Check if results file exists
        if 's3_key_result_file' not in job:
            print(f"No results file found for job {job_id}")
            return True
        
        # Archive the results file
        try:
            archive_id = archive_to_glacier(
                S3_RESULTS_BUCKET,
                job['s3_key_result_file'],
                GLACIER_VAULT
            )
            
            # Update DynamoDB with archive ID
            # Reference: DynamoDB update_item - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET results_file_archive_id = :archive_id',
                ExpressionAttributeValues={
                    ':archive_id': archive_id
                }
            )
            
            print(f"Successfully archived job {job_id} for user {user_id}")
            return True
            
        except Exception as e:
            print(f"Error archiving job {job_id}: {str(e)}")
            # Don't delete the message so it can be retried
            return False
            
    except json.JSONDecodeError as e:
        print(f"Error parsing message JSON: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error processing message: {str(e)}")
        return False

def main():
    """Main function to poll SQS queue for archive requests"""
    print(f"Starting archive service, polling queue: {SQS_ARCHIVE_QUEUE}")
    
    # Get queue URL
    queue_url = get_queue_url(SQS_ARCHIVE_QUEUE)
    if not queue_url:
        print(f"Could not find SQS queue: {SQS_ARCHIVE_QUEUE}")
        return
    
    while True:
        try:
            # Poll for messages
            # Reference: SQS receive_message with long polling - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=SQS_MAX_MESSAGES,
                WaitTimeSeconds=SQS_WAIT_TIME,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    print(f"Processing archive message: {message['MessageId']}")
                    
                    # Process the message
                    success = process_archive_message(message, queue_url)
                    
                    if success:
                        # Delete message after successful processing
                        # Reference: SQS delete_message - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print(f"Deleted message: {message['MessageId']}")
                
        except ClientError as e:
            print(f"AWS error: {str(e)}")
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            time.sleep(5)

if __name__ == '__main__':
    main()