CommerceFlow Architecture Decisions
This document records important architectural decisions made during the development of CommerceFlow.
ADR-001 — Use Amazon S3 as the Data Lake
Decision
Use Amazon S3 as the primary storage layer for raw, processed, invalid, and curated data.
Context
CommerceFlow needs durable storage for incoming source data and transformed analytical datasets.
Reason
S3 provides scalable object storage and integrates naturally with Glue, Athena, Lambda, EventBridge, and other AWS services.
Trade-off
S3 is object storage rather than a traditional database, so schema management and transactional behavior must be handled by other components.
ADR-002 — Use a Dataset-Ready _SUCCESS Manifest
Decision
Use _SUCCESS as the signal that a complete dataset is ready for processing.
Context
A dataset consists of multiple files. Triggering processing on every individual file could start the pipeline before the complete dataset is available.
Reason
The manifest provides a clear dataset-level readiness signal.
Trade-off
The ingestion process must reliably create _SUCCESS only after all required files have been successfully written.
ADR-003 — Use EventBridge for Event-Driven Processing
Decision
Use Amazon EventBridge to react to dataset-ready events.
Context
The platform should automatically start processing when new data becomes available.
Reason
EventBridge removes the need for continuous polling and provides a clean event-driven integration point.
Trade-off
Event-driven systems introduce additional configuration and require careful event filtering and failure handling.
ADR-004 — Use Step Functions for Workflow Orchestration
Decision
Use AWS Step Functions to orchestrate the data pipeline.
Context
The pipeline contains multiple dependent processing stages.
Reason
Step Functions provides state management, retries, error handling, and workflow visibility.
Trade-off
For a very small pipeline, Step Functions introduces additional complexity compared with directly invoking a Lambda or Glue job.
ADR-005 — Use Lambda for Lightweight Validation
Decision
Use AWS Lambda for lightweight input validation.
Context
Some validation can be performed before launching distributed processing.
Reason
Lambda is appropriate for short-running validation and routing logic.
Trade-off
Lambda is not appropriate for large-scale distributed transformations.
ADR-006 — Use AWS Glue/PySpark for Large-Scale Transformation
Decision
Use AWS Glue with PySpark for data transformation.
Context
Orders need to be joined with customer and product data and aggregated into analytical datasets.
Reason
Glue provides managed Spark processing without requiring the project to maintain its own Spark cluster.
Trade-off
Glue has startup time and compute costs, so it should not be used for every small processing task.
ADR-007 — Store Curated Data as Parquet
Decision
Store curated analytical datasets in Parquet format.
Context
Curated data will primarily be queried analytically.
Reason
Parquet provides columnar storage, compression, and efficient analytical queries.
Trade-off
Parquet is less convenient than CSV for manual inspection and direct editing.
ADR-008 — Use Glue Data Catalog
Decision
Use AWS Glue Data Catalog for metadata management.
Context
Athena and Glue need consistent metadata describing datasets stored in S3.
Reason
Glue Data Catalog integrates directly with the AWS analytics ecosystem.
Trade-off
Schema evolution and crawler behavior must be managed carefully.
ADR-009 — Use Athena for Serverless Analytics
Decision
Use Amazon Athena for SQL-based analysis of curated S3 data.
Context
The project needs an analytical query layer without maintaining database servers.
Reason
Athena provides serverless SQL querying directly against S3.
Trade-off
Poor partitioning or querying large amounts of unnecessary data can increase query cost.
ADR-010 — Separate Human Access from Workload Roles
Decision
Human development access and AWS workload permissions will be separated.
Context
Developers require broad enough permissions to build infrastructure, while workloads should only access the resources they need.
Reason
This follows least-privilege principles and reduces the impact of credential compromise.
Trade-off
More IAM roles and policies must be managed.
ADR-011 — Use CDK for Infrastructure as Code
Decision
Use AWS CDK to codify the stable CommerceFlow infrastructure.
Context
Manually created resources are difficult to reproduce and maintain.
Reason
CDK allows infrastructure to be defined as code, version controlled, reviewed, and deployed consistently.
Trade-off
CDK adds an abstraction layer and requires understanding both AWS resources and infrastructure code.
ADR-012 — Use OIDC for GitHub Actions
Decision
Use GitHub Actions OIDC federation instead of long-lived AWS access keys for CI/CD.
Context
CI/CD pipelines need AWS access to deploy infrastructure.
Reason
OIDC allows GitHub Actions to obtain temporary AWS credentials without storing permanent AWS access keys in GitHub.
Trade-off
The IAM trust relationship requires careful repository and branch restrictions.