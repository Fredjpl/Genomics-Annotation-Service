# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  # Reference: Flask request args - https://flask.palletsprojects.com/en/2.0.x/api/#flask.Request.args
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Validate required parameters
  # Reference: Flask abort - https://flask.palletsprojects.com/en/2.0.x/api/#flask.abort
  if not bucket_name or not s3_key:
    app.logger.error("Missing required parameters: bucket or key")
    return abort(400)

  # Extract the job ID from the S3 key
  # The key format is: "peilij/user_id/UUID~original_filename.vcf"
  try:
    # Split the key to extract components
    key_parts = s3_key.split('/')
    if len(key_parts) < 3:
      raise ValueError("Invalid key structure")
    
    file_part = key_parts[-1]  # "UUID~file.vcf"
    if '~' not in file_part:
      raise ValueError("Missing delimiter in filename")
    
    job_id, input_file_name = file_part.split('~', 1)
    
    # Validate job_id is a valid UUID
    # Reference: UUID validation - https://docs.python.org/3/library/uuid.html
    uuid.UUID(job_id)  # This will raise ValueError if invalid
    
  except ValueError as e:
    app.logger.error(f"Invalid S3 key format: {s3_key}. Error: {str(e)}")
    return abort(400)

  # Get the authenticated user's ID from the session
  # Reference: Flask session - https://flask.palletsprojects.com/en/2.0.x/api/#flask.session
  user_id = session.get('primary_identity')
  if not user_id:
    app.logger.error("No user ID found in session")
    return abort(403)
  
  # Get the user's profile to extract email
  user_profile = get_profile(identity_id=user_id)
  user_email = user_profile.email if user_profile else None
  
  # Persist job to database
  try:
    # Get DynamoDB table
    # Reference: Boto3 DynamoDB Resource - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    
    # Create timestamp
    # Reference: Python time - https://docs.python.org/3/library/time.html#time.time
    submit_time = int(time.time())
    
    # Create job item
    job_item = {
      'job_id': job_id,
      'user_id': user_id,  # Using authenticated user's ID
      'input_file_name': input_file_name,
      's3_inputs_bucket': bucket_name,
      's3_key_input_file': s3_key,
      'submit_time': submit_time,
      'job_status': 'PENDING',
      'user_email': user_email
    }
    
    # Put item in DynamoDB
    # Reference: DynamoDB put_item - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
    table.put_item(Item=job_item)
          
    # Reference: Python logging - https://docs.python.org/3/library/logging.html
    app.logger.info(f"Job {job_id} saved to database for user {user_id}")
    
  except ClientError as e:
    # Reference: Boto3 error handling - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
    error_code = e.response['Error']['Code']
    if error_code == 'ResourceNotFoundException':
      app.logger.error(f"DynamoDB table not found: {app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']}")
    elif error_code == 'ValidationException':
      app.logger.error(f"Invalid data format for DynamoDB: {e}")
    else:
      app.logger.error(f"Error saving job to database: {e}")
    return abort(500)
  except Exception as e:
    app.logger.error(f"Unexpected error saving to database: {str(e)}")
    return abort(500)

  # Send message to request queue
  try:
    # Get SNS client
    # Reference: Boto3 SNS Client - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html
    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    
    # Validate SNS topic ARN exists
    if not app.config.get('AWS_SNS_JOB_REQUEST_TOPIC'):
      app.logger.error("SNS topic ARN not configured")
      return abort(500)
    
    # Create message
    message = {
      'job_id': job_id,
      'user_id': user_id,  # Using authenticated user's ID
      's3_inputs_bucket': bucket_name,
      's3_key_input_file': s3_key,
      'submit_time': submit_time,
      'input_file_name': input_file_name,
      'user_email': user_email
    }
    
    # Convert message to JSON
    # Reference: JSON dumps - https://docs.python.org/3/library/json.html#json.dumps
    try:
      message_json = json.dumps(message)
    except (TypeError, ValueError) as e:
      app.logger.error(f"Error serializing message to JSON: {e}")
      return abort(500)
    
    # Publish to SNS topic
    # Reference: SNS publish - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    response = sns.publish(
      TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
      Message=message_json,
      Subject='New Annotation Job Request'
    )
    
    # Validate SNS response
    if 'MessageId' not in response:
      app.logger.error("SNS publish did not return MessageId")
      return abort(500)
    
    app.logger.info(f"Published job {job_id} to SNS topic with MessageId: {response['MessageId']}")
    
  except ClientError as e:
    # Reference: Boto3 ClientError - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html#parsing-error-responses-and-catching-exceptions-from-aws-services
    error_code = e.response['Error']['Code']
    if error_code == 'NotFound':
      app.logger.error(f"SNS topic not found: {app.config['AWS_SNS_JOB_REQUEST_TOPIC']}")
    elif error_code == 'InvalidParameter':
      app.logger.error(f"Invalid parameter for SNS publish: {e}")
    elif error_code == 'AuthorizationError':
      app.logger.error(f"Not authorized to publish to SNS topic: {e}")
    else:
      app.logger.error(f"Error publishing to SNS: {e}")
    return abort(500)
  except Exception as e:
    # Reference: Python exception handling - https://docs.python.org/3/tutorial/errors.html
    app.logger.error(f"Unexpected error publishing to SNS: {str(e)}")
    return abort(500)

  # Reference: Flask render_template - https://flask.palletsprojects.com/en/2.0.x/api/#flask.render_template
  return render_template('annotate_confirm.html', job_id=job_id)

"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  # Get the authenticated user's ID from the session
  user_id = session.get('primary_identity')
  
  if not user_id:
    app.logger.error("No user ID found in session")
    return abort(403)
  
  try:
    # Initialize DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    
    # Query DynamoDB for all jobs belonging to this user
    # Reference: DynamoDB Query - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
    # Reference: DynamoDB Key Conditions - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#boto3.dynamodb.conditions.Key
    # Reference: DynamoDB Global Secondary Index - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
    response = table.query(
      IndexName='user_id_index', 
      KeyConditionExpression=Key('user_id').eq(user_id),
      # Sort by submit time in descending order (newest first)
      # Reference: ScanIndexForward - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ScanIndexForward
      ScanIndexForward=False
    )
    
    # Extract items from response
    # Reference: Query Response Structure - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_ResponseSyntax
    annotations = response.get('Items', [])
    
    # Convert epoch timestamps to human-readable format
    # Reference: Python datetime - https://docs.python.org/3/library/datetime.html
    for annotation in annotations:
      if 'submit_time' in annotation:
        # Create timezone-aware datetime object for CST (UTC-5)
        # Reference: Python timezone - https://docs.python.org/3/library/datetime.html#timezone-objects
        # Reference: Python timedelta - https://docs.python.org/3/library/datetime.html#timedelta-objects
        cst = timezone(-timedelta(hours=5))
        # Convert epoch to datetime object
        # Reference: datetime.fromtimestamp - https://docs.python.org/3/library/datetime.html#datetime.datetime.fromtimestamp
        submit_datetime = datetime.fromtimestamp(annotation['submit_time'], cst)
        # Format as readable string
        # Reference: strftime - https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
        annotation['submit_time'] = submit_datetime.strftime('%Y-%m-%d %H:%M:%S')

    app.logger.info(f"Retrieved {len(annotations)} annotations for user {user_id}")

    # Return the template with annotations
    return render_template('annotations.html', annotations=annotations)
    
  except ClientError as e:
    error_code = e.response['Error']['Code']
    error_message = e.response['Error']['Message']
    
    # Reference: DynamoDB Error Codes - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
    if error_code == 'ResourceNotFoundException':
      app.logger.error(f"DynamoDB table not found: {app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']}")
    elif error_code == 'ValidationException':
      # This might occur if the GSI doesn't exist
      # Reference: DynamoDB ValidationException - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-IndexName
      app.logger.error(f"Query validation error: {error_message}")
    elif error_code == 'ProvisionedThroughputExceededException':
      # Reference: DynamoDB Throughput - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughputExceeded.html
      app.logger.error(f"DynamoDB throughput exceeded: {error_message}")
    else:
      app.logger.error(f"DynamoDB error: {error_code} - {error_message}")
    
    # Return empty list on error
    return render_template('annotations.html', annotations=[])
    
  except Exception as e:
    # Catch any unexpected errors
    app.logger.error(f"Unexpected error retrieving annotations: {str(e)}")
    return render_template('annotations.html', annotations=[])



"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  # Get the authenticated user's ID from the session
  user_id = session.get('primary_identity')
  
  if not user_id:
      app.logger.error("No user ID found in session")
      return abort(403)
  
  try:
      # Initialize DynamoDB resource
      dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
      table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
      
      # Get the annotation job from DynamoDB
      # Reference: DynamoDB get_item - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
      response = table.get_item(
          Key={'job_id': id}
      )
      
      # Check if item exists
      if 'Item' not in response:
          app.logger.error(f"Job {id} not found")
          return abort(404)
      
      annotation = response['Item']
      
      # Verify the job belongs to the authenticated user
      if annotation.get('user_id') != user_id:
          app.logger.error(f"User {user_id} not authorized to view job {id}")
          return render_template('error.html',
              title='Not authorized',
              alert_level='danger',
              message="Not authorized to view this job"
          ), 403
      
      # Convert epoch timestamps to human-readable format in CST (UTC-5)
      cst = timezone(-timedelta(hours=5))
      
      # Convert submit_time
      if 'submit_time' in annotation:
          submit_datetime = datetime.fromtimestamp(annotation['submit_time'], cst)
          annotation['submit_time'] = submit_datetime.strftime('%Y-%m-%d %H:%M:%S')
      
      # Convert complete_time if job is completed
      if 'complete_time' in annotation and annotation.get('job_status') == 'COMPLETED':
          complete_time_epoch = annotation['complete_time']
          complete_datetime = datetime.fromtimestamp(annotation['complete_time'], cst)
          annotation['complete_time'] = complete_datetime.strftime('%Y-%m-%d %H:%M:%S')
      
      # Generate presigned URLs for file downloads
      # Reference: Boto3 Config - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
      s3 = boto3.client('s3',
          region_name=app.config['AWS_REGION_NAME'],
          config=Config(signature_version='s3v4'))
      
      # Generate presigned URL for input file download
      # Reference: S3 generate_presigned_url - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
      try:
          input_file_url = s3.generate_presigned_url(
              'get_object',
              Params={
                  'Bucket': annotation.get('s3_inputs_bucket', app.config['AWS_S3_INPUTS_BUCKET']),
                  'Key': annotation['s3_key_input_file']
              },
              ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
          )
          annotation['input_file_url'] = input_file_url
      except ClientError as e:
          app.logger.error(f"Error generating presigned URL for input file: {e}")
          annotation['input_file_url'] = "#"
          
      # If job is completed, check if results are available
      if annotation.get('job_status') == 'COMPLETED':
          # Check if user is free and if free access period has expired
          user_role = session.get('role', 'free_user')
          free_access_expired = False
          
          if user_role == 'free_user' and 'complete_time' in response['Item']:
              # Check if more than 5 minutes have passed since completion
              current_time = int(time.time())
              # Parse the string to datetime object and then convert back to epoch time
              elapsed_time = current_time - complete_time_epoch
              # 5 minutes = 300 seconds
              if elapsed_time > 300:
                  free_access_expired = True
                  app.logger.info(f"Free access expired for job {id} (elapsed: {elapsed_time}s)")

          # If not expired or user is premium, generate presigned URL for results
          if not free_access_expired and 's3_key_result_file' in annotation:
              if annotation.get('results_file_archive_id') and user_role == 'premium_user':
                  # File was archived but user is now premium
                  if annotation.get('restore_job_id'):
                      # Restoration is in progress
                      restore_status = annotation.get('restore_status', 'PENDING')
                      if restore_status == 'PENDING':
                          annotation['restore_message'] = "Results file is being restored from archive. Please check back later."
                      elif restore_status == 'FAILED':
                          annotation['restore_message'] = "Results file restoration failed. Please contact support."
                  else:
                      # File is still archived but no restoration started
                      # This shouldn't happen if subscription process works correctly
                      annotation['restore_message'] = "Results file is archived. Restoration will begin shortly."
              else:
                  # File is not archived or being restored - generate normal download URL
                  try:
                      result_file_url = s3.generate_presigned_url(
                          'get_object',
                          Params={
                              'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                              'Key': annotation['s3_key_result_file']
                          },
                          ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
                      )
                      annotation['result_file_url'] = result_file_url
                  except ClientError as e:
                      app.logger.error(f"Error generating presigned URL for results file: {e}")
                      # Check if the error is because file doesn't exist in S3
                      if e.response['Error']['Code'] == 'NoSuchKey':
                          annotation['restore_message'] = "Results file not found. It may have been archived."

          # Pass the free_access_expired flag to template
          return render_template('annotation_details.html', 
                                annotation=annotation, 
                                free_access_expired=free_access_expired)
      
      # For running jobs or jobs without free access issues
      return render_template('annotation_details.html', annotation=annotation)
      
  except ClientError as e:
      app.logger.error(f"DynamoDB error: {e}")
      return abort(500)
  except Exception as e:
      app.logger.error(f"Unexpected error: {str(e)}")
      return abort(500)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    # Get the authenticated user's ID from the session
    user_id = session.get('primary_identity')
    
    if not user_id:
        app.logger.error("No user ID found in session")
        return abort(403)
    
    try:
        # Initialize DynamoDB resource
        dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        
        # Get the annotation job from DynamoDB
        response = table.get_item(
            Key={'job_id': id}
        )
        
        # Check if item exists
        if 'Item' not in response:
            app.logger.error(f"Job {id} not found")
            return abort(404)
        
        annotation = response['Item']
        
        # Verify the job belongs to the authenticated user
        if annotation.get('user_id') != user_id:
            app.logger.error(f"User {user_id} not authorized to view log for job {id}")
            return render_template('error.html',
                title='Not authorized',
                alert_level='danger',
                message="Not authorized to view this job's log"
            ), 403
        
        # Check if job is completed and log file exists
        if annotation.get('job_status') != 'COMPLETED':
            app.logger.error(f"Job {id} is not completed yet")
            return render_template('error.html',
                title='Log not available',
                alert_level='warning',
                message="Log file is only available for completed jobs"
            ), 400
        
        # Check if log file key exists
        if 's3_key_log_file' not in annotation:
            app.logger.error(f"No log file key found for job {id}")
            return render_template('error.html',
                title='Log not found',
                alert_level='warning',
                message="Log file not found for this job"
            ), 404
        
        # Initialize S3 client
        s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
        
        try:
            # Get the log file from S3
            # Reference: S3 get_object - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
            log_response = s3.get_object(
                Bucket=app.config['AWS_S3_RESULTS_BUCKET'],
                Key=annotation['s3_key_log_file']
            )
            
            # Read the log file contents
            # Reference: StreamingBody read - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#streamingbody
            log_file_contents = log_response['Body'].read().decode('utf-8')
            
            app.logger.info(f"Successfully retrieved log file for job {id}")
            
            # Render the log view template
            return render_template('view_log.html',
                job_id=id,
                log_file_contents=log_file_contents
            )
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'NoSuchKey':
                app.logger.error(f"Log file not found in S3 for job {id}: {annotation['s3_key_log_file']}")
                return render_template('error.html',
                    title='Log file not found',
                    alert_level='warning',
                    message="The log file could not be found"
                ), 404
            elif error_code == 'AccessDenied':
                app.logger.error(f"Access denied to log file for job {id}: {error_message}")
                return render_template('error.html',
                    title='Access denied',
                    alert_level='danger',
                    message="Access denied to the log file"
                ), 403
            else:
                app.logger.error(f"S3 error retrieving log file: {error_code} - {error_message}")
                return abort(500)
                
        except UnicodeDecodeError as e:
            # Reference: Python Unicode Error - https://docs.python.org/3/library/exceptions.html#UnicodeDecodeError
            app.logger.error(f"Error decoding log file for job {id}: {str(e)}")
            return render_template('error.html',
                title='Log file error',
                alert_level='warning',
                message="Error reading log file contents"
            ), 500
            
    except ClientError as e:
        # DynamoDB errors
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'ResourceNotFoundException':
            app.logger.error(f"DynamoDB table not found: {app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']}")
        elif error_code == 'ValidationException':
            app.logger.error(f"DynamoDB validation error: {error_message}")
        else:
            app.logger.error(f"DynamoDB error: {error_code} - {error_message}")
        
        return abort(500)
        
    except Exception as e:
        # Catch any unexpected errors
        app.logger.error(f"Unexpected error retrieving log for job {id}: {str(e)}")
        return abort(500)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    user_id = session['primary_identity']
    
    try:
      # Initialize AWS services
      dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
      table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
      sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
      
      # Query for all user's jobs that have been archived
      # Reference: DynamoDB Query with Filter - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
      response = table.query(
        IndexName='user_id_index',
        KeyConditionExpression=Key('user_id').eq(user_id),
        FilterExpression=Attr('results_file_archive_id').exists()
      )
      
      archived_jobs = response.get('Items', [])
      app.logger.info(f"Found {len(archived_jobs)} archived jobs for user {user_id}")
      
      if archived_jobs:
        # Get the restoration topic ARN
        restore_topic_arn =  app.config['AWS_SNS_RESTORE_TOPIC']

        if restore_topic_arn:
          # Send restoration requests for each archived job
          for job in archived_jobs:
            restore_message = {
              'job_id': job['job_id'],
              'user_id': user_id,
              'archive_id': job['results_file_archive_id'],
              's3_key_result_file': job.get('s3_key_result_file', '')
            }
            
            # Publish restoration request
            sns.publish(
              TopicArn=restore_topic_arn,
              Message=json.dumps(restore_message),
              Subject=f'Restore request for job {job["job_id"]}'
            )
            
          app.logger.info(f"Initiated restoration for {len(archived_jobs)} jobs")
        else:
          app.logger.error(f"Restore topic not found: {app.config['AWS_SNS_RESTORE_TOPIC']}")
      
    except ClientError as e:
      app.logger.error(f"Error initiating restoration: {e}")
      # Don't fail the subscription if restoration fails
    except Exception as e:
      app.logger.error(f"Unexpected error during restoration: {str(e)}")
      # Don't fail the subscription if restoration fails

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
