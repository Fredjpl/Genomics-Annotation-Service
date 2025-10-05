# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
import time
import boto3
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# AWS configuration
REGION = config.get('aws', 'AwsRegionName')
S3_RESULTS_BUCKET = config.get('s3', 'ResultsBucket')
GLACIER_VAULT = config.get('glacier', 'VaultName')
DYNAMO_TABLE = config.get('dynamodb', 'AnnotationsTable')
SQS_THAW_QUEUE = config.get('sqs', 'ThawQueue')
SQS_WAIT_TIME = config.getint('sqs', 'WaitTimeSeconds')
SQS_MAX_MESSAGES = config.getint('sqs', 'MaxNumberOfMessages')
CHECK_INTERVAL = config.getint('thaw', 'CheckIntervalSeconds')

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

def check_restoration_status(vault_name, restore_job_id):
    """Check if a Glacier restoration job is complete"""
    try:
        # Describe the job to check its status
        # Reference: Glacier describe_job - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.describe_job
        response = glacier.describe_job(
            vaultName=vault_name,
            jobId=restore_job_id
        )
        
        job_status = response.get('StatusCode')
        print(f"Restoration job {restore_job_id} status: {job_status}")
        
        return job_status == 'Succeeded'
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            print(f"Restoration job {restore_job_id} not found")
        else:
            print(f"Error checking job status: {str(e)}")
        return False

def download_and_upload_to_s3(vault_name, restore_job_id, s3_bucket, s3_key):
    """Download from Glacier and upload to S3"""
    try:
        # Get the job output (the restored archive)
        # Reference: Glacier get_job_output - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
        print(f"Downloading restored archive from Glacier job {restore_job_id}")
        
        response = glacier.get_job_output(
            vaultName=vault_name,
            jobId=restore_job_id
        )
        
        # Read the archive data
        archive_data = response['body'].read()
        
        # Upload to S3
        # Reference: S3 put_object - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
        print(f"Uploading to S3: {s3_bucket}/{s3_key}")
        
        s3.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=archive_data,
            ServerSideEncryption='AES256'
        )
        
        print(f"Successfully uploaded restored file to S3")
        return True
        
    except ClientError as e:
        print(f"Error downloading/uploading restored archive: {str(e)}")
        return False

def delete_glacier_archive(vault_name, archive_id):
    """Delete an archive from Glacier"""
    try:
        # Delete the archive from Glacier
        # Reference: Glacier delete_archive - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.delete_archive
        glacier.delete_archive(
            vaultName=vault_name,
            archiveId=archive_id
        )
        
        print(f"Deleted archive {archive_id} from Glacier")
        return True
        
    except ClientError as e:
        print(f"Error deleting archive from Glacier: {str(e)}")
        # Don't fail the process if deletion fails
        return False

def process_thaw_message(message, queue_url):
    """Process a thaw monitoring message"""
    try:
        # Parse message
        body = json.loads(message['Body'])
        
        # Check if from SNS
        if 'Message' in body:
            thaw_data = json.loads(body['Message'])
        else:
            thaw_data = body
        
        job_id = thaw_data.get('job_id')
        restore_job_id = thaw_data.get('restore_job_id')
        archive_id = thaw_data.get('archive_id')
        s3_key = thaw_data.get('s3_key_result_file')
        vault_name = thaw_data.get('vault_name', GLACIER_VAULT)
        tier = thaw_data.get('tier', 'Standard')
        initiated_time = thaw_data.get('initiated_time', 0)
        
        if not all([job_id, restore_job_id, archive_id, s3_key]):
            print(f"Missing required fields in thaw message: {thaw_data}")
            return False
        
        print(f"Checking restoration status for job {job_id}")
        
        # Check if restoration is complete
        if check_restoration_status(vault_name, restore_job_id):
            # Download from Glacier and upload to S3
            if download_and_upload_to_s3(vault_name, restore_job_id, S3_RESULTS_BUCKET, s3_key):
                # Delete the archive from Glacier
                delete_glacier_archive(vault_name, archive_id)
                
                # Update DynamoDB - remove archive ID and restore job ID
                # Reference: DynamoDB update_item with remove - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
                try:
                    table.update_item(
                        Key={'job_id': job_id},
                        UpdateExpression='REMOVE results_file_archive_id, restore_job_id, restore_tier, restore_status',
                        ConditionExpression='attribute_exists(results_file_archive_id)'
                    )
                    print(f"Successfully restored job {job_id} from Glacier")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                        print(f"Job {job_id} archive ID already removed")
                    else:
                        print(f"Error updating DynamoDB: {str(e)}")
                
                return True
            else:
                print(f"Failed to upload restored file for job {job_id}")
                return False
        else:
            # Not ready yet, check how long we've been waiting
            current_time = int(time.time())
            elapsed_time = current_time - initiated_time
            
            # Expedited should complete within 5 minutes, Standard within 5 hours
            # Reference: Glacier retrieval options - https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html
            max_wait_time = 300 if tier == 'Expedited' else 18000  # 5 min or 5 hours
            
            if elapsed_time > max_wait_time:
                print(f"Restoration timeout for job {job_id} ({tier} tier, waited {elapsed_time}s)")
                # Update status to failed
                table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET restore_status = :status',
                    ExpressionAttributeValues={':status': 'FAILED'}
                )
                return True  # Remove from queue
            
            # Requeue the message to check again later
            # Reference: SQS send_message with delay - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message
            delay_seconds = min(CHECK_INTERVAL, 900)  # Max SQS delay is 900 seconds
            
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(thaw_data),
                DelaySeconds=delay_seconds
            )
            
            print(f"Restoration not ready for job {job_id}, checking again in {delay_seconds}s")
            return True
            
    except json.JSONDecodeError as e:
        print(f"Error parsing message JSON: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error processing thaw message: {str(e)}")
        return False

def main():
    """Main function to monitor Glacier restoration jobs"""
    print(f"Starting thaw service, polling queue: {SQS_THAW_QUEUE}")
    
    # Get queue URL
    queue_url = get_queue_url(SQS_THAW_QUEUE)
    if not queue_url:
        print(f"Could not find SQS queue: {SQS_THAW_QUEUE}")
        return
    
    while True:
        try:
            # Poll for messages
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=SQS_MAX_MESSAGES,
                WaitTimeSeconds=SQS_WAIT_TIME,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    print(f"Processing thaw message: {message['MessageId']}")
                    
                    # Process the message
                    success = process_thaw_message(message, queue_url)
                    
                    if success:
                        # Delete message after processing
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