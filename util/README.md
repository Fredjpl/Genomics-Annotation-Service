This directory should contain the following utility-related files:
* `helpers.py` - Miscellaneous helper functions
* `util_config.py` - Common configuration options for all utilities

Each utility should be in its own sub-directory, along with its configuration file, as follows:

/archive
* `archive.py` - Archives free user result files to Glacier
* `archive_config.ini` - Configuration options for archive utility

/notify
* `notify.py` - Sends notification email on completion of annotation job
* `notify_config.ini` - Configuration options for notification utility

/restore
* `restore.py` - Initiates restore of Glacier archive(s)
* `restore_config.ini` - Configuration options for restore utility

/thaw
* `thaw.py` - Saves recently restored archive(s) to S3
* `thaw_config.ini` - Configuration options for thaw utility

If you completed Ex. 14, include your annotator load testing script here
* `ann_load.py` - Annotator load testing script


## Archive Process Implementation

### Overview
The archive process automatically moves result files from S3 to AWS Glacier for free users after 5 minutes of job completion. This reduces storage costs while maintaining data availability for potential future restoration.

### Architecture
The archive system uses a message-driven architecture with the following components:

1. **SNS Topic (`peilij_archive_topic`)**: Receives notifications when annotation jobs complete
2. **SQS Queue (`peilij_archive_queue`)**: Buffers archive requests for processing
3. **Archive Service (`archive.py`)**: Polls the queue and performs archival operations

### Detailed Workflow

1. **Job Completion Trigger**
   - When an annotation job completes, `run.py` publishes a message to both the results topic and the archive topic
   - The message includes job metadata: `job_id`, `user_id`, `complete_time`, and file locations

2. **Time-Based Archival Logic**
   - `archive.py` receives messages from the SQS queue
   - For each message, it calculates the elapsed time since job completion
   - If less than 5 minutes have passed, the message is requeued with a delay
   - This ensures the 5-minute grace period for free users to download their results

3. **User Role Verification**
   - Before archiving, the service checks the user's current role (not the role at submission time)
   - Premium users' files are never archived, even if they were free users when submitting the job
   - This prevents archiving files for users who upgrade within the grace period

4. **Archival Process**
   - Files are downloaded from S3 (`mpcs-cc-gas-results`)
   - Uploaded to Glacier vault (`mpcs-cc`)
   - The Glacier archive ID is stored in DynamoDB for future restoration
   - Original file is deleted from S3 to save costs

### Key Design Decisions

- **Message Requeuing**: Instead of using a scheduler or cron job, messages are requeued with delays to implement the 5-minute waiting period. This approach is more scalable and event-driven.
- **Current Role Checking**: The system checks the user's role at archive time, not submission time, allowing users who upgrade quickly to keep their files accessible.
- **Idempotency**: The system checks if files are already archived before processing, preventing duplicate archival operations.

## Restore Process Implementation

### Overview
The restore process handles the recovery of archived files when free users upgrade to premium. It implements graceful degradation by attempting expedited retrieval first, then falling back to standard retrieval if necessary.

### Architecture
The restore system uses a two-stage process with multiple services:

1. **Restore Service (`restore.py`)**: Initiates Glacier retrieval jobs
2. **Thaw Service (`thaw.py`)**: Monitors retrieval jobs and moves restored files back to S3
3. **SNS/SQS Infrastructure**: Coordinates communication between services

### Detailed Workflow

1. **Upgrade Trigger**
   - When a user upgrades to premium, the web application sends restore requests for all archived files
   - Each archived job generates a message to the restore queue

2. **Restoration Initiation (`restore.py`)**
   - Attempts expedited retrieval first (1-5 minutes completion time)
   - If expedited retrieval fails due to insufficient capacity, automatically falls back to standard retrieval (3-5 hours)
   - Creates a Glacier retrieval job and stores the job ID in DynamoDB
   - Publishes a message to the thaw topic for monitoring

3. **Restoration Monitoring (`thaw.py`)**
   - Periodically checks the status of Glacier retrieval jobs
   - When a job completes:
     - Downloads the restored file from Glacier
     - Uploads it back to the S3 results bucket
     - Deletes the archive from Glacier
     - Removes archive-related fields from DynamoDB
   - If a job isn't ready, requeues the message with a delay

4. **Graceful Degradation**
   - The system handles expedited retrieval failures gracefully
   - Users experience faster restoration when possible, but the system remains reliable under high load
   - Timeout mechanisms prevent infinite waiting for failed jobs

### Key Design Decisions

- **Two-Stage Process**: Separating initiation from monitoring allows for better scalability and failure handling. The restore service can quickly process many requests while the thaw service handles the long-running monitoring tasks.

- **Message-Based Coordination**: Using SNS/SQS for communication between services provides:
  - Loose coupling between components
  - Built-in retry mechanisms
  - Scalability through multiple service instances
  - Durability of restoration requests

- **Graceful Degradation**: Attempting expedited retrieval first provides better user experience when available, while automatic fallback ensures reliability.

## Rationale for Architecture Choices

### Why Message Queues?

1. **Decoupling**: Services operate independently, improving system resilience
2. **Scalability**: Multiple instances can process messages concurrently
3. **Reliability**: SQS provides automatic message persistence and retry logic
4. **Asynchronous Processing**: Long-running operations don't block other system components

### Why Time-Based Requeuing Instead of Scheduled Jobs?

1. **Event-Driven**: More responsive to actual job completions
2. **Distributed**: No single point of failure like a cron scheduler
3. **Elastic**: Scales naturally with message volume
4. **Precise Timing**: Each job gets exactly 5 minutes from its completion time

## Error Handling and Resilience

- **Idempotent Operations**: Archive checks prevent duplicate archival
- **Conditional DynamoDB Updates**: Prevent race conditions
- **Message Visibility Timeout**: Failed operations return messages to queue
- **Graceful Error Recovery**: Services continue operating despite individual message failures
- **Comprehensive Logging**: All operations are logged for debugging

