# Genomics Annotation Service (GAS)

A scalable, fault-tolerant Software-as-a-Service platform for genomics annotation, built on AWS. GAS lets researchers upload VCF files, run annotation jobs, and retrieve results through a secure web interface — with automatic scaling, tiered subscriptions, event-driven processing, and cost-optimized long-term storage.

> Built as the capstone for MPCS 51083 Cloud Computing at the University of Chicago.

---

## Architecture

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

<img width="663" height="566" alt="image" src="https://github.com/user-attachments/assets/b431ee3a-86e0-43d0-b8f5-6f9a422fe07e" />

<img width="651" height="709" alt="image" src="https://github.com/user-attachments/assets/7dd77259-4523-4013-a9f5-2b4b6b92fef6" />


### Component breakdown

| Layer | AWS Service | Role |
|---|---|---|
| Web tier | EC2 + ELB + Auto Scaling | Stateless Flask/Gunicorn nodes behind an Application Load Balancer |
| Annotator farm | EC2 + Auto Scaling | Workers that consume jobs from SQS and run AnnTools |
| Hot object store | S3 | Input files, result files, job logs |
| Cold object store | Glacier | Archival tier for Free-user data |
| Job metadata | DynamoDB | Per-job status, timestamps, Glacier archive IDs |
| User accounts | RDS (PostgreSQL) | Profile + subscription state, accessed via SQLAlchemy/Alembic |
| Messaging | SNS + SQS | Decouples submission, processing, archival, and notification |
| Notifications | Lambda + SES | Serverless email on job completion |
| Secrets | AWS Secrets Manager | DB credentials with rotation |
| AuthN/AuthZ | Globus Auth (OAuth2) | Federated login (e.g., UChicago CNetID) |

---

## Features

- **Tiered subscriptions** — Free users capped on job size with 5-minute result retention; Premium users unlimited.
- **End-to-end HTTPS** — Enforced across the app; TLS termination at the ELB via ACM.
- **Event-driven pipeline** — Submission → SNS → SQS → Annotator → SNS → Lambda → SES. No synchronous blocking between tiers.
- **Graceful degradation** — Glacier restore first attempts Expedited retrieval, falls back to Standard on capacity failure.
- **Elastic compute** — Web tier scales on ELB request rate; annotator tier scales on SQS queue depth.
- **Self-healing** — Terminating every instance triggers ASG recovery; the system rebuilds itself from user-data scripts with zero manual intervention.

---

## Engineering Highlights

### Decoupled, event-driven processing
All major components communicate asynchronously through SNS topics and SQS queues. A slow annotator never backs up the web tier, and a web-tier deploy never drops in-flight jobs.

### Cost-optimized storage lifecycle
Free-tier results are archived to Glacier 5 minutes after completion via a delayed-message pattern. On upgrade, a two-phase thaw kicks in:
- `restore.py` initiates the retrieval (Expedited → Standard fallback)
- `thaw.py` polls, moves the restored object back to S3, and cleans up the vault
- Glacier archive IDs are persisted in DynamoDB to maintain a reliable bidirectional mapping

### Elastic compute with CloudWatch-driven policies
- **Web ASG** — scale out on 2XX response rate exceeding threshold; scale in on sub-10ms target response time
- **Annotator ASG** — scale out on `NumberOfMessagesSent > 50/10min`; scale in on `< 5/10min`
- Verified self-healing by terminating all `-web` and `-ann` instances and watching the environment fully rebuild

### Zero-hardcoding discipline
Bucket names, queue names, table names, and credentials are externalized into:
- `config.py` (Flask) via environment variables from `.env`
- `ann_config.ini`, `archive_config.ini`, `restore_config.ini`, `thaw_config.ini` (utility scripts) via `ConfigParser`
- **AWS Secrets Manager** for DB credentials (never in source)

### Automated, reproducible deployment
ASG launch templates pull the latest source bundle from S3 and start the service from EC2 user-data — no SSH, no snowflake instances. A new region/account can be spun up from scratch.

---

## Tech Stack

**Languages** Python 3, HTML/Jinja2, Bash
**Web** Flask, Gunicorn (multi-worker WSGI), Bootstrap
**Data** DynamoDB, PostgreSQL, SQLAlchemy, Alembic
**AWS** EC2, S3, Glacier, ELB, Auto Scaling, CloudWatch, Lambda, SES, SNS, SQS, RDS, Secrets Manager, ACM
**IaC** Terraform (web/annotator/utility infrastructure, SNS/SQS, Lambda)
**Testing** Locust (load testing), custom SQS load-generation script

---

## Project Structure

```
gas/
├── web/                        # Flask web application
│   ├── views.py                # Routes and business logic
│   ├── config.py               # Runtime configuration
│   ├── templates/              # Jinja2 templates
│   └── run_gas.sh              # Gunicorn launcher
├── ann/                        # Annotator service
│   ├── annotator.py            # SQS consumer → AnnTools
│   ├── run.py                  # Post-processing + SNS publish
│   └── ann_config.ini
├── util/
│   ├── archive/                # Free-user archival to Glacier
│   ├── restore/                # Glacier retrieval initiator
│   ├── thaw/                   # Glacier → S3 mover
│   ├── helpers.py
│   └── ann_load.py             # Annotator load generator
├── aws/                        # EC2 user-data scripts
│   ├── user_data_web_server.txt
│   └── user_data_annotator.txt
└── terraform/                  # Infrastructure as code
```

---

## Load Testing Results

Simulated 200–300 concurrent users at 20–30 req/sec using Locust against the ELB-fronted web tier:

- Web ASG scaled from 2 → near max (10) instances within ~2–3 minutes of sustained load
- Scale-in followed the configured cooldown window rather than reacting instantly — intentional, to avoid thrashing
- Annotator ASG scaled independently based on SQS queue depth, validating the decoupling between tiers

Key insight: CloudWatch-driven autoscaling lags observable user latency by 60–90 seconds in this setup. In production, I would pair reactive alarms with predictive scaling (or a smaller scaling step with shorter cooldown) to smooth over spike traffic.

---

## What I Took Away

- **Designing for failure** — every component can die, and the system must keep working
- **Thinking in queues and events** rather than synchronous calls
- **Cost vs. latency tradeoffs** — e.g., Expedited vs. Standard Glacier retrieval, ASG cooldown tuning
- **Idempotent bootstrapping** — encoding ops runbooks into user-data so infrastructure is disposable
- **Multi-tier access control** — OAuth2 federation + application-level role checks for Free/Premium separation

---

## License

Academic project. Not for commercial use.
