import os
import sys
import time
import shutil
import boto3
# Import Key for DynamoDB operations: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#boto3.dynamodb.conditions.Key
from boto3.dynamodb.conditions import Key
import driver
from configparser import ConfigParser
import json


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")


# Get configuration
config = ConfigParser()
config.read('ann_config.ini')

# AWS configuration from config file
REGION = config.get('aws', 'AwsRegionName')
S3_RESULTS_BUCKET = config.get('s3', 'ResultsBucket')
DYNAMO_TABLE = config.get('dynamodb', 'AnnotationsTable')
SNS_RESULTS_TOPIC = config.get('sns', 'JobResultsTopic')
SNS_ARCHIVE_TOPIC = config.get('sns', 'ArchiveTopic')

# Initialize AWS services
s3 = boto3.client('s3', region_name=REGION)
dynamo = boto3.resource('dynamodb', region_name=REGION)
table = dynamo.Table(DYNAMO_TABLE)
sns = boto3.client('sns', region_name=REGION)

# Get the topic ARN by name: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.list_topics
def get_topic_arn(topic_name):
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

def main():
    # Check command-line arguments: https://docs.python.org/3/library/sys.html#sys.argv
    if len(sys.argv) != 4:
        print("Usage: hw5_run.py <directory_prefix> <job_id> <local_input_path>")
        sys.exit(1)

    directory_prefix, job_id, local_input_path = sys.argv[1:]

    # Prepare working directory and move into it
    # Path manipulation: https://docs.python.org/3/library/os.path.html
    job_dir = os.path.dirname(local_input_path)
    job_root = os.path.abspath(job_dir)

    # Change working directory: https://docs.python.org/3/library/os.html#os.chdir
    os.chdir(job_root)

    # Derive filenames
    orig_filename = os.path.basename(local_input_path)
    base, _ = os.path.splitext(orig_filename)
    annotated_filename = f"{base}.annot.vcf"
    log_filename = f"{base}.vcf.count.log"
    log_path = os.path.join(job_dir, log_filename)
    annotated_path = os.path.join(job_dir, annotated_filename)

    # Run annotation with timing
    with Timer():
        driver.run(orig_filename, 'vcf')

    # Wait for result files to be created
    while not os.path.exists(annotated_filename):
        time.sleep(5)
        print(f"Waiting for {annotated_path} to be created...")

    # Format S3 keys for result files
    result_key = f"{directory_prefix}/{job_id}~{annotated_filename}"
    log_key = f"{directory_prefix}/{job_id}~{log_filename}"
    
    try:
        # Upload result files to S3: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
        s3.upload_file(annotated_filename, S3_RESULTS_BUCKET, result_key)
        s3.upload_file(log_filename, S3_RESULTS_BUCKET, log_key)
        
        # Update the job item in DynamoDB with results
        # DynamoDB update_item: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
        current_time = int(time.time())

        # First, get the user_id from the job record (we'll need it for the notification)
        response = table.get_item(Key={'job_id': job_id})
        if 'Item' not in response:
            print(f"Error: Could not find job {job_id} in database")
            sys.exit(1)

        job_item = response['Item']
        user_id = job_item.get('user_id', 'unknown')
        user_email = job_item.get('user_email', '')  
        input_file_name = orig_filename
        
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET s3_results_bucket = :results_bucket, s3_key_result_file = :result_key, s3_key_log_file = :log_key, complete_time = :complete_time, job_status = :completed',
            ExpressionAttributeValues={
                ':results_bucket': S3_RESULTS_BUCKET,
                ':result_key': result_key,
                ':log_key': log_key,
                ':complete_time': current_time,
                ':completed': 'COMPLETED'
            }
        )
        
        print(f"Updated job {job_id} status to COMPLETED in DynamoDB")

        # Publish notification to SNS topic
        topic_arn = get_topic_arn(SNS_RESULTS_TOPIC)
        if topic_arn:
            # Prepare the notification message
            notification_data = {
                'job_id': job_id,
                'user_id': user_id,
                'user_email': user_email,
                'input_file_name': input_file_name,
                's3_results_bucket': S3_RESULTS_BUCKET,
                's3_key_result_file': result_key,
                's3_key_log_file': log_key,
                'complete_time': current_time,
                'job_status': 'COMPLETED'
            }
            
            # Publish to SNS
            try:
                response = sns.publish(
                    TopicArn=topic_arn,
                    Message=json.dumps(notification_data),
                    Subject=f'Annotation job {job_id} completed'
                )
                print(f"Published completion notification for job {job_id} to SNS topic")
            except Exception as e:
                print(f"Error publishing to SNS: {str(e)}")
                # Don't fail the job if notification fails

            # Also publish to archive topic for potential archiving
            archive_topic_arn = get_topic_arn(SNS_ARCHIVE_TOPIC)
            if archive_topic_arn:
                try:
                    response = sns.publish(
                        TopicArn=archive_topic_arn,
                        Message=json.dumps(notification_data),
                        Subject=f'Archive check for job {job_id}'
                    )
                    print(f"Published archive notification for job {job_id}")
                except Exception as e:
                    print(f"Error publishing to archive topic: {str(e)}")
        
    except boto3.exceptions.Boto3Error as e:
        # Error handling for AWS operations: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        print(f"Error uploading results or updating database: {str(e)}")
        sys.exit(1)

    # Clean up local job directory: https://docs.python.org/3/library/shutil.html#shutil.rmtree
    shutil.rmtree(job_root)
    print(f"Uploaded {annotated_filename} and {log_filename} to S3 bucket {S3_RESULTS_BUCKET} with key {result_key} and {log_key}.")
    print(f"Removed local job directory {job_root}.")

if __name__ == '__main__':
    main()