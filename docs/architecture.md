CommerceFlow Architecture
1. Overview
CommerceFlow is an event-driven e-commerce data platform built on AWS.
The platform ingests customer, product, and order data, validates the incoming datasets, transforms the data into analytics-ready formats, and exposes the curated data for analytics and future application consumption.
The project is designed as a production-style AWS data platform rather than a collection of isolated AWS service demonstrations.
2. Business Objective
CommerceFlow simulates an e-commerce data platform where business data is generated and processed through a reliable, scalable, and observable data pipeline.
The platform should support:
- Customer analytics
- Product performance analysis
- Sales and revenue analysis
- Daily sales reporting
- Customer-level sales analysis
- Product/category performance
- Data-quality monitoring
- Pipeline failure detection and recovery
3. High-Level Architecture
                    DATA SOURCES
                         |
                         v
                  +-------------+
                  |   S3 Raw    |
                  | Data Lake   |
                  +-------------+
                         |
                    _SUCCESS
                         |
                         v
                  +-------------+
                  | EventBridge |
                  +-------------+
                         |
                         v
                  +-------------+
                  | Step        |
                  | Functions   |
                  +-------------+
                         |
                         v
                  +-------------+
                  | Lambda      |
                  | Validation  |
                  +-------------+
                    |         |
              Valid |         | Invalid
                    |         v
                    |    +-----------+
                    |    | Invalid / |
                    |    | Quarantine|
                    |    +-----------+
                    |
                    v
              +-------------+
              | AWS Glue    |
              | PySpark     |
              +-------------+
                    |
                    v
              +-------------+
              | S3 Curated  |
              | Parquet     |
              +-------------+
                    |
                    v
              +-------------+
              | Glue Data   |
              | Catalog     |
              +-------------+
                    |
                    v
              +-------------+
              | Athena      |
              +-------------+
                    |
          +---------+---------+
          |                   |
          v                   v
     Analytics           Future API
                              |
                              v
                        React Dashboard
4. Core Data Domains
CommerceFlow initially contains three primary data domains.
Customers
Field	Description
customer_id	Unique customer identifier
name	Customer name
email	Customer email
city	Customer city
state	Customer state
registration_date	Customer registration date


Products
Field	Description
product_id	Unique product identifier
product_name	Product name
category	Product category
price	Current product price


Orders
Field	Description
order_id	Unique order identifier
customer_id	Customer reference
product_id	Product reference
quantity	Number of units ordered
unit_price	Price captured at order time
order_status	Order lifecycle status
order_date	Order date


Order statuses:
PLACED
CONFIRMED
SHIPPED
DELIVERED
CANCELLED
Historical Pricing
unit_price is stored directly on the order.
This prevents historical revenue calculations from changing when the current product price changes.
For example:
Product current price: ₹3,500
Historical order price: ₹3,200

Order.unit_price = ₹3,200
5. Data Lake Design
The primary data lake is Amazon S3.
Logical zones:
commerceflow/
│
├── raw/
│   ├── customers/
│   ├── products/
│   └── orders/
│
├── processed/
│
├── curated/
│   ├── customer_sales/
│   ├── product_sales/
│   └── daily_sales/
│
├── invalid/
│
└── checkpoints/
Raw Zone
The raw zone preserves incoming data in its original form.
Purpose:
- Auditability
- Replay
- Troubleshooting
- Recovery
- Reprocessing
Raw data should not be modified after ingestion.
Processed Zone
The processed zone contains validated and transformed intermediate datasets.
Curated Zone
The curated zone contains analytics-ready datasets, primarily in Parquet format.
Examples:
customer_sales
product_sales
daily_sales
Invalid Zone
Records that fail validation are separated from valid data.
This allows the pipeline to continue processing valid records while preserving invalid records for investigation.
6. Dataset Readiness
CommerceFlow uses a _SUCCESS manifest to indicate that a complete dataset is ready for processing.
Example:
dt=2026-10-07/

customers.csv
products.csv
orders.csv
_SUCCESS
The _SUCCESS file is written last.
The pipeline should react to the dataset-ready signal rather than triggering processing for every individual file.
This prevents processing incomplete datasets.
7. Event-Driven Processing
Amazon EventBridge is responsible for detecting dataset readiness events.
The intended flow is:
S3 _SUCCESS
      |
      v
EventBridge
      |
      v
Step Functions
Event-driven processing reduces unnecessary polling and allows the platform to react automatically when new data becomes available.
8. Workflow Orchestration
AWS Step Functions orchestrates the pipeline.
Example workflow:
START
  |
  v
Validate Input
  |
  v
Run Data Quality Checks
  |
  +---- Invalid ----> Quarantine + Notify
  |
  v
Run Glue Transformation
  |
  v
Validate Output
  |
  v
Update Metadata
  |
  v
SUCCESS
Step Functions provides:
- Workflow visibility
- Retry handling
- Error handling
- State tracking
- Service integration
- Controlled execution flow
9. Data Validation
AWS Lambda performs lightweight validation before large-scale transformation.
Example checks:
- Required fields
- Valid identifiers
- Valid dates
- Positive quantity
- Positive unit price
- Valid order status
- Basic schema validation
- Dataset completeness
Examples of intentionally invalid records in the development dataset include:
Negative quantity
Zero quantity
Missing customer_id
Unknown customer_id
Unknown product_id
Future order date
Duplicate order_id
Negative unit_price
Invalid customer email
Negative product price
Lambda is used for lightweight validation.
Large-scale transformation is handled by AWS Glue/PySpark.
10. AWS Glue and PySpark
AWS Glue performs distributed data transformation.
Typical processing flow:
S3 Raw
   |
   v
Glue
   |
   +-- Join Orders + Customers
   |
   +-- Join Orders + Products
   |
   +-- Calculate Revenue
   |
   +-- Aggregate Sales
   |
   v
S3 Curated
Example calculation:
revenue = quantity * unit_price
Potential curated datasets:
Customer Sales
customer_id
customer_name
total_orders
total_quantity
total_revenue
Product Sales
product_id
product_name
category
total_orders
total_quantity
total_revenue
Daily Sales
order_date
total_orders
total_quantity
total_revenue
11. Storage Format
Raw source files are initially CSV because they represent the incoming source data.
Curated datasets will use Parquet.
Reasons:
- Columnar storage
- Efficient analytical queries
- Compression
- Reduced Athena scan costs
- Better performance for large datasets
- Natural integration with Spark
12. Glue Data Catalog
AWS Glue Data Catalog provides metadata for datasets stored in S3.
It will maintain information such as:
- Database
- Tables
- Columns
- Data types
- S3 locations
- Partition information
Example:
commerceflow_catalog
│
├── customers
├── products
├── orders
├── customer_sales
├── product_sales
└── daily_sales
13. Athena
Amazon Athena provides SQL-based analytical access to curated S3 data.
Example query:
SELECT
    order_date,
    SUM(quantity * unit_price) AS revenue
FROM daily_sales
GROUP BY order_date;
Potential business questions:
- What was total revenue today?
- Which products generate the most revenue?
- Which customers have the highest lifetime sales?
- Which categories perform best?
- What is the cancellation rate?
- How does daily revenue change over time?
14. Error Handling
CommerceFlow separates data-quality failures from infrastructure failures.
Data-quality failure
Invalid record
      |
      v
Invalid Zone
      |
      v
Continue valid processing
      |
      v
Notification
Processing failure
Glue failure
      |
      v
Step Functions Retry
      |
      +---- Success
      |
      +---- Failure
              |
              v
         Failure Handler
              |
              v
         CloudWatch / SNS
Raw data remains available for replay and investigation.
15. Security
Security follows the principle of least privilege.
Human access
Development access is provided through the dedicated:
awanish-workspace
Administrative operations remain separated under:
awanish-admin
Workload access
AWS services use IAM roles instead of embedded access keys.
Examples:
Glue Job
   -> Glue IAM Role

Lambda
   -> Lambda Execution Role

Step Functions
   -> Step Functions Execution Role

EC2
   -> EC2 Instance Role
Encryption
S3 and other supported services should use encryption.
AWS KMS can be introduced where customer-managed encryption keys are required.
16. Observability
Amazon CloudWatch provides:
- Lambda logs
- Glue logs
- Step Functions execution visibility
- Metrics
- Alarms
Amazon SNS can be used for operational notifications.
Example:
Glue Failure
     |
     v
CloudWatch
     |
     v
Alarm
     |
     v
SNS
     |
     v
Notification
17. Infrastructure as Code
AWS CDK will eventually define the CommerceFlow infrastructure.
Target resources include:
- S3
- IAM
- Lambda
- EventBridge
- Step Functions
- Glue
- Athena
- CloudWatch
- SNS
- KMS
The project will initially validate the architecture manually where useful, then codify the stable architecture with CDK.
This avoids writing infrastructure code before the service dependencies and design decisions are understood.
18. CI/CD
GitHub Actions will eventually automate:
GitHub
   |
   v
Pull Request
   |
   v
Tests / Lint / Validation
   |
   v
Build
   |
   v
CDK Synth
   |
   v
Deployment
Deployment should use short-lived AWS credentials through OIDC rather than storing long-lived AWS access keys in GitHub.
19. Future Application Layer
CommerceFlow can later expose curated analytics through a Spring Boot API.
Architecture:
React
  |
  v
API Gateway
  |
  v
Spring Boot
  |
  v
Athena / Curated Data
Potential APIs:
GET /api/sales/daily
GET /api/customers/top
GET /api/products/top
GET /api/categories/performance
GET /api/customers/{id}/sales
20. Future React Dashboard
A React/Vite/Tailwind frontend can consume the API.
Potential dashboard components:
- Total revenue
- Total orders
- Customer count
- Daily sales
- Top products
- Top customers
- Category performance
- Pipeline health
The frontend is an extension of the data platform rather than the core pipeline itself.
21. Scalability
The architecture is designed to scale independently at each major stage.
S3
 |
 +--> EventBridge
 |
 +--> Step Functions
 |
 +--> Glue / Spark
 |
 +--> Parquet
 |
 +--> Athena
S3 provides scalable storage.
Glue provides distributed processing.
Athena provides serverless analytics.
Step Functions provides scalable workflow orchestration.
The architecture avoids introducing infrastructure that is unnecessary for the initial workload.
22. Cost Considerations
The project is designed with AWS cost awareness.
Key practices:
- Use small Glue workers during development
- Stop/terminate unused EC2 resources
- Avoid unnecessary always-on infrastructure
- Use Parquet for analytical data
- Partition datasets appropriately
- Monitor CloudWatch usage
- Configure AWS Budgets
- Delete temporary resources after testing
The objective is to demonstrate production-style architecture without creating unnecessary ongoing AWS costs.
23. Project Evolution
CommerceFlow will evolve through these stages:
1. Architecture and foundation
2. S3 data lake
3. Glue Catalog
4. Glue/PySpark transformation
5. Lambda validation
6. EventBridge integration
7. Step Functions orchestration
8. Athena analytics
9. CDK infrastructure
10. Production readiness
11. Spring Boot API
12. React dashboard
The first ten stages form the core data platform.
The API and frontend are application extensions.
24. Definition of Done
CommerceFlow is considered complete when:
- Data can be ingested into S3
- Dataset readiness is detected automatically
- Validation identifies invalid data
- Invalid data is isolated
- Valid data is transformed using Glue/PySpark
- Curated data is stored in Parquet
- Metadata is available through Glue Data Catalog
- Athena can query curated datasets
- Workflow failures are retried and reported
- CloudWatch provides operational visibility
- IAM follows least-privilege principles
- Infrastructure is reproducible through CDK
- CI/CD can validate and deploy the infrastructure
- The platform can be consumed by future applications
25. Design Philosophy
CommerceFlow is intentionally designed around the following progression:
Architecture
     ↓
Data Flow
     ↓
Service Integration
     ↓
Reliability
     ↓
Security
     ↓
Observability
     ↓
Infrastructure as Code
     ↓
CI/CD
     ↓
Application Consumption
The goal is not to demonstrate individual AWS services in isolation.
The goal is to demonstrate how AWS services work together to build a realistic, reliable, secure, and maintainable data platform.