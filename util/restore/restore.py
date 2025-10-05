# restore.py
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
config.read('restore_config.ini')

# AWS configuration
REGION = config.get('aws', 'AwsRegionName')
GLACIER_VAULT = config.get('glacier', 'VaultName')
DYNAMO_TABLE = config.get('dynamodb', 'AnnotationsTable')
SQS_RESTORE_QUEUE = config.get('sqs', 'RestoreQueue')
SQS_THAW_QUEUE = config.get('sqs', 'ThawQueue')
SNS_THAW_TOPIC = config.get('sns', 'ThawTopic')
SQS_WAIT_TIME = config.getint('sqs', 'WaitTimeSeconds')
SQS_MAX_MESSAGES = config.getint('sqs', 'MaxNumberOfMessages')

# Initialize AWS services
glacier = boto3.client('glacier', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(DYNAMO_TABLE)
sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)

def get_queue_url(queue_name):
    """Get SQS queue URL by name"""
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']
    except ClientError as e:
        print(f"Error getting queue URL: {str(e)}")
        return None

def get_topic_arn(topic_name):
    """Get SNS topic ARN by name"""
    try:
        topics = sns.list_topics()['Topics']
        for topic in topics:
            arn = topic['TopicArn']
            if arn.endswith(':' + topic_name):
                return arn
        print(f"Warning: SNS topic {topic_name} not found")
        return None
    except Exception as e:
        print(f"Error finding SNS topic: {str(e)}")
        return None

def initiate_glacier_restoration(vault_name, archive_id, job_id):
    """Initiate a Glacier retrieval job with graceful degradation"""
    try:
        # First, try expedited retrieval
        # Reference: Glacier initiate_job - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
        print(f"Attempting expedited retrieval for archive {archive_id}")
        
        job_params = {
            'Type': 'archive-retrieval',
            'ArchiveId': archive_id,
            'Tier': 'Expedited',
            'Description': f'Restore for job {job_id}'
        }
        
        response = glacier.initiate_job(
            vaultName=vault_name,
            jobParameters=job_params
        )
        
        retrieval_job_id = response['jobId']
        print(f"Initiated expedited retrieval job: {retrieval_job_id}")
        return retrieval_job_id, 'Expedited'
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        # If expedited fails due to insufficient capacity, fall back to standard
        # Reference: Glacier error handling - https://docs.aws.amazon.com/amazonglacier/latest/dev/api-error-responses.html
        if error_code == 'InsufficientCapacityException':
            print(f"Expedited retrieval unavailable, falling back to standard")
            
            try:
                # Try standard retrieval
                job_params['Tier'] = 'Standard'
                response = glacier.initiate_job(
                    vaultName=vault_name,
                    jobParameters=job_params
                )
                
                retrieval_job_id = response['jobId']
                print(f"Initiated standard retrieval job: {retrieval_job_id}")
                return retrieval_job_id, 'Standard'
                
            except ClientError as e2:
                print(f"Error initiating standard retrieval: {str(e2)}")
                raise
        else:
            print(f"Error initiating retrieval: {str(e)}")
            raise

def process_restore_message(message):
    """Process a restore request message"""
    try:
        # Parse message
        body = json.loads(message['Body'])
        
        # Check if from SNS
        if 'Message' in body:
            restore_data = json.loads(body['Message'])
        else:
            restore_data = body
        
        job_id = restore_data.get('job_id')
        user_id = restore_data.get('user_id')
        archive_id = restore_data.get('archive_id')
        s3_key = restore_data.get('s3_key_result_file')
        
        if not all([job_id, user_id, archive_id]):
            print(f"Missing required fields in restore message: {restore_data}")
            return False
        
        print(f"Processing restore request for job {job_id}")
        
        # Check if restoration is already in progress
        response = table.get_item(Key={'job_id': job_id})
        
        if 'Item' not in response:
            print(f"Job {job_id} not found in database")
            return False
        
        job = response['Item']
        
        # Check if already being restored
        if 'restore_job_id' in job:
            print(f"Job {job_id} already has restoration in progress")
            return True
        
        # Initiate Glacier restoration
        try:
            retrieval_job_id, tier = initiate_glacier_restoration(
                GLACIER_VAULT,
                archive_id,
                job_id
            )
            
            # Update DynamoDB with retrieval job ID
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET restore_job_id = :restore_job_id, restore_tier = :tier, restore_status = :status',
                ExpressionAttributeValues={
                    ':restore_job_id': retrieval_job_id,
                    ':tier': tier,
                    ':status': 'PENDING'
                }
            )
            
            # Send message to thaw queue for monitoring
            thaw_topic_arn = get_topic_arn(SNS_THAW_TOPIC)
            if thaw_topic_arn:
                thaw_message = {
                    'job_id': job_id,
                    'restore_job_id': retrieval_job_id,
                    'archive_id': archive_id,
                    's3_key_result_file': s3_key,
                    'tier': tier,
                    'vault_name': GLACIER_VAULT,
                    'initiated_time': int(time.time())
                }
                
                # Publish to thaw topic
                sns.publish(
                    TopicArn=thaw_topic_arn,
                    Message=json.dumps(thaw_message),
                    Subject=f'Monitor restoration for job {job_id}'
                )
                
                print(f"Sent restoration monitoring request for job {job_id}")
            
            return True
            
        except Exception as e:
            print(f"Error initiating restoration for job {job_id}: {str(e)}")
            return False
            
    except json.JSONDecodeError as e:
        print(f"Error parsing message JSON: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error processing restore message: {str(e)}")
        return False

def main():
    """Main function to poll SQS queue for restore requests"""
    print(f"Starting restore service, polling queue: {SQS_RESTORE_QUEUE}")
    
    # Get queue URL
    queue_url = get_queue_url(SQS_RESTORE_QUEUE)
    if not queue_url:
        print(f"Could not find SQS queue: {SQS_RESTORE_QUEUE}")
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
                    print(f"Processing restore message: {message['MessageId']}")
                    
                    # Process the message
                    success = process_restore_message(message)
                    
                    if success:
                        # Delete message after successful processing
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