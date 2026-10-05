# CommerceFlow

An end-to-end e-commerce data platform on AWS, built to get hands-on experience with how AWS services work together.

- **Cloud:** AWS
- **Repository:** `commerce-flow`
- **Goal:** trace data from the moment it arrives to the moment it shows up in an API and a dashboard.

---

## 1. Goal and Scope

CommerceFlow answers one question: how do you design, automate, operate, and expose an AWS data platform from start to finish?

**Data lifecycle:**

```text
Ingest -> Validate -> Process -> Curate -> Catalog -> Query -> API -> Dashboard
```

**Business scenario.** An e-commerce business sends customer, product, and order files. The platform must:

1. Ingest new files and keep the original data.
2. Check file and row quality, and keep bad data separate.
3. Transform valid data into analytics-ready datasets.
4. Make the data queryable with SQL.
5. Handle failures without losing data silently.
6. Show what the pipeline is doing (logs, alerts).
7. Deploy the same way every time (IaC, CI/CD).
8. Later, expose analytics through an API and a dashboard.

**Out of scope for now:** payments, real user authentication, mobile app, Kubernetes, Kafka/streaming, machine learning, microservices, real-time order management.

---

## 2. Design Principles

- **Keep raw data.** Raw files are never changed or deleted, so any run can be replayed.
- **Fail visibly.** Bad data and failed jobs are recorded and alerted, never ignored.
- **Right tool for the job.** Lambda for small logic, Glue/Spark for heavy transforms, Step Functions for orchestration.
- **Least privilege.** Each workload gets its own IAM role with only what it needs.
- **Idempotent runs.** Running the same input twice must give the same result, with no duplicates.
- **Cost aware.** No always-on resources unless needed.
- **No unnecessary services.** If a service does not solve a real requirement, leave it out.

---

## 3. Assumptions

Change these as the project grows.

| Item | Assumption |
|---|---|
| Data size | About 100k customers, 5k products, 1M orders (synthetic, generated with Python + Faker) |
| File format | CSV in the raw zone |
| Arrival | One batch per day, all three files together |
| Region | One AWS region (choose one and use it everywhere) |
| Environment | Single dev environment at first |
| Users | One developer, so low traffic on the API |

---

## 4. Architecture

```text
 Data source (CSV files + _SUCCESS manifest)
        |
        v
  S3 raw/  ----(object created)---->  EventBridge
                                          |
                                          v
                                   Step Functions
                                          |
                          +---------------+----------------+
                          |                                |
                   Lambda: file checks                     |
                          |                                |
                 invalid file --> S3 invalid/ + SNS        |
                          |                                |
                        valid                              |
                          |                                |
                          v                                |
                  Glue job (PySpark)  <--------------------+
                  - row-level checks
                  - bad rows --> S3 invalid/
                  - join + enrich
                  - write Parquet
                          |
                          v
                   S3 curated/ (Parquet)
                          |
                          v
                   Glue Data Catalog
                          |
                          v
                       Athena
                          |
                          v
        Spring Boot API --> React dashboard   (Phases 10-11)
```

**Cross-cutting services:** IAM, CloudWatch, SNS, (KMS if needed), AWS CDK, GitHub Actions.

### Service responsibilities

| Service | Responsibility |
|---|---|
| S3 | Storage for all zones |
| EventBridge | Routes the "new data arrived" event |
| Step Functions | Runs the workflow, retries, failure paths |
| Lambda | File-level checks, small helper tasks, notifications |
| Glue (PySpark) | Row-level checks, joins, aggregations, Parquet output |
| Glue Data Catalog | Table metadata for Athena |
| Athena | SQL queries over curated data |
| CloudWatch | Logs, metrics, alarms |
| SNS | Alerts for invalid data and failures |
| CDK | Infrastructure as code |
| GitHub Actions | Tests and deployment |

Lambda is not used for large transforms. That is on purpose.

---

## 5. Key Design Decisions

These fix problems that would otherwise show up during the build. Record each one in `docs/decisions.md`.

### 5.1 Triggering with three files (race condition)

The Glue job joins customers, products, and orders. If it starts when only one file arrives, it runs on incomplete data.

**Decision:** the source uploads all three files, then uploads a `_SUCCESS` manifest file last. Only the manifest triggers the workflow.

Alternatives: a scheduled daily run, or per-dataset processing with the join job running only when all three are ready.

Set Glue job max concurrency to **1** so two runs never write to the same output.

### 5.2 Where validation happens

| Layer | Checks |
|---|---|
| **Lambda (file-level)** | File name pattern, extension, size, header row, expected columns |
| **Glue (row-level)** | Data types, nulls, duplicates, `quantity <= 0`, `price <= 0`, invalid dates, orders that reference a missing customer or product |

Row-level checks need the other datasets and may be large, so they run in Spark. Bad rows go to `invalid/` with a reason column. Good rows continue.

### 5.3 Idempotency and reprocessing

Reprocessing is a feature, so duplicates must be prevented.

**Decision:** each run is tied to a `run_date`. The Glue job overwrites the output **partition** for that date (dynamic partition overwrite). Re-running the same date replaces the old output instead of adding to it.

Do not rely on job bookmarks alone for this.

### 5.4 Curated layer has a row-level table

Aggregates alone are not enough. Add one enriched fact table and build the aggregates from it.

- `fact_orders`: one row per order, with customer and product details joined in. Partitioned by `order_date`.
- `customer_sales`, `product_sales`, `daily_sales`: built from `fact_orders`.

This also teaches partitioning and partition pruning in Athena.

### 5.5 Athena behind an API

Athena queries take seconds and are billed by data scanned, so calling it on every dashboard page load is a poor design.

**Decision:** the API caches Athena results (in memory, or in S3/DynamoDB) and the dashboard reads cached results. Precomputed aggregates are small, so this works well.

Other options: load aggregates into PostgreSQL or DynamoDB for serving.

The API must use the AWS SDK for Java v2 `AthenaClient` with an IAM role. No access keys.

### 5.6 Where Spring Boot runs

Decide before Phase 10. Options: Lambda (cheapest, cold starts), App Runner (simple, always-on cost), ECS Fargate (more setup). For this project, start with **App Runner or a container on Lambda**, and check the cost before choosing.

### 5.7 Crawler vs. explicit tables

Use a crawler once to learn what it does. For curated tables, define the schema explicitly (later in CDK) and use **partition projection** or `ALTER TABLE ADD PARTITION`. This shows you what the crawler was doing for you.

---

## 6. Data Model

### 6.1 Source data

**customers.csv**

| Column | Type | Notes |
|---|---|---|
| customer_id | string | Primary key, not null |
| name | string | Not null |
| email | string | Valid format |
| city | string | |
| state | string | |
| registration_date | date | `YYYY-MM-DD` |

**products.csv**

| Column | Type | Notes |
|---|---|---|
| product_id | string | Primary key, not null |
| product_name | string | Not null |
| category | string | Not null |
| price | decimal(10,2) | Must be > 0 |

**orders.csv**

| Column | Type | Notes |
|---|---|---|
| order_id | string | Primary key, not null |
| customer_id | string | Must exist in customers |
| product_id | string | Must exist in products |
| quantity | int | Must be > 0 |
| order_date | date | `YYYY-MM-DD`, not in the future |

### 6.2 Curated tables (Parquet)

**fact_orders** (partitioned by `order_date`)

| Column | Type |
|---|---|
| order_id | string |
| customer_id | string |
| customer_name | string |
| city | string |
| state | string |
| product_id | string |
| product_name | string |
| category | string |
| unit_price | decimal(10,2) |
| quantity | int |
| line_total | decimal(12,2) |
| order_date | date (partition) |

**customer_sales**: `customer_id`, `customer_name`, `total_orders`, `total_quantity`, `total_spend`

**product_sales**: `product_id`, `product_name`, `category`, `total_orders`, `total_quantity`, `total_revenue`

**daily_sales** (partitioned by `date`): `date`, `total_orders`, `total_revenue`, `avg_order_value`

### 6.3 Invalid records

Stored under `invalid/` with the original columns plus: `source_file`, `run_date`, `failed_rule`, `error_detail`.

---

## 7. S3 Layout and Naming

### 7.1 Naming convention

| Resource | Pattern | Example |
|---|---|---|
| Bucket | `commerceflow-<env>-<account-id>-<region>` | `commerceflow-dev-123456789012-ap-south-1` |
| Lambda | `cf-<env>-<purpose>` | `cf-dev-file-validator` |
| Glue job | `cf-<env>-<purpose>` | `cf-dev-transform-orders` |
| Step Functions | `cf-<env>-pipeline` | `cf-dev-pipeline` |
| IAM role | `cf-<env>-<workload>-role` | `cf-dev-glue-role` |
| Glue database | `cf_<env>` | `cf_dev` |

### 7.2 Layout

```text
commerceflow-<env>-.../
+-- raw/
|   +-- customers/dt=YYYY-MM-DD/customers.csv
|   +-- products/dt=YYYY-MM-DD/products.csv
|   +-- orders/dt=YYYY-MM-DD/orders.csv
|   +-- _manifests/dt=YYYY-MM-DD/_SUCCESS
+-- processed/        (intermediate output, if needed)
+-- curated/
|   +-- fact_orders/order_date=YYYY-MM-DD/
|   +-- customer_sales/
|   +-- product_sales/
|   +-- daily_sales/
+-- invalid/
|   +-- files/        (files that failed Lambda checks)
|   +-- rows/         (rows that failed Glue checks)
+-- athena-results/
+-- scripts/          (Glue job scripts)
```

### 7.3 Zones

| Zone | Purpose | Rules |
|---|---|---|
| raw | Original files | Never modified. Versioning on. |
| processed | Intermediate output | Can be rebuilt. |
| curated | Analytics-ready Parquet | Overwritten by partition. |
| invalid | Rejected files and rows | Kept for investigation. Lifecycle rule to expire after N days. |

---

## 8. Event and Workflow

### 8.1 Event

S3 must have **EventBridge notifications enabled**. An EventBridge rule matches only the manifest key (`_SUCCESS` under `raw/_manifests/`).

Sample event (trimmed):

```json
{
  "version": "0",
  "source": "aws.s3",
  "detail-type": "Object Created",
  "detail": {
    "bucket": { "name": "commerceflow-dev-123456789012-ap-south-1" },
    "object": { "key": "raw/_manifests/dt=2026-01-15/_SUCCESS", "size": 0 }
  }
}
```

The workflow reads `dt=2026-01-15` from the key and builds all paths from it. No file path is hardcoded.

The rule target needs a **dead-letter queue (SQS)** for events that cannot be delivered.

### 8.2 Workflow (Step Functions)

```text
START
  -> Parse event (get run_date)
  -> Lambda: file-level checks on the 3 files
        invalid -> move to invalid/files, SNS alert, END (failed)
  -> Start Glue job (run_date) and wait
        failure -> retry (backoff, max attempts)
        retries exhausted -> failure handler: SNS + CloudWatch, END (failed)
  -> Verify catalog / add partitions
  -> END (success)
```

Every failure path ends in a clear state with an alert. No execution should end silently.

---

## 9. Glue Job (PySpark)

1. Read the three raw datasets for `run_date`.
2. Check schema and types.
3. Apply row-level rules. Write rejected rows to `invalid/rows/` with `failed_rule`.
4. Remove duplicates (by primary key).
5. Handle nulls (reject or default, per rule).
6. Join orders with customers and products.
7. Calculate `line_total`.
8. Write `fact_orders` as Parquet, partitioned by `order_date`, with partition overwrite.
9. Build and write the three aggregate tables from `fact_orders`.

Keep the transformation logic in plain Python functions so it can be tested with local Spark.

---

## 10. Analytics (Athena)

Use a dedicated **workgroup** with a per-query scan limit and a fixed result location.

Questions the platform should answer:

- Which customers generate the most revenue?
- Which products and categories perform best?
- What is the daily revenue and daily order count?
- What is the average order value?

Keep the SQL in `sql/`. Compare the data scanned for a query with and without a partition filter.

---

## 11. Security

- **IAM:** a separate role for Glue, Lambda, Step Functions, CDK deployment, and CI/CD. Scope S3 permissions to specific prefixes.
- **S3:** block public access, default encryption (SSE-S3), versioning on raw, bucket policy requiring TLS.
- **Secrets:** nothing hardcoded in code, GitHub, or config. Use Secrets Manager or SSM Parameter Store if secrets are needed.
- **CI/CD access:** GitHub Actions authenticates to AWS with OIDC, not long-lived keys.
- **KMS:** customer-managed keys only if there is a clear requirement. SSE-S3 is enough to start.

---

## 12. Observability

CloudWatch should answer:

- Did the event arrive and did the workflow start?
- Did file validation pass?
- Did Glue start and finish? How long did it take?
- Where did a run fail?
- How often does the pipeline fail?

Set up:

- Log retention on every log group (for example 14 days).
- Alarms: Step Functions failed executions, Glue job failures, Lambda errors, DLQ not empty.
- SNS email for alarms.
- One small dashboard with these signals. No extra dashboards.

---

## 13. Failure and Recovery

| Failure | Handling |
|---|---|
| Bad file (wrong format or headers) | Move to `invalid/files`, alert, stop run |
| Bad rows | Written to `invalid/rows`, run continues |
| Glue job failure | Retry with backoff, then failure handler (SNS + CloudWatch) |
| EventBridge delivery failure | Goes to DLQ |
| Bug found after a run | Fix, then re-run the same `run_date`. Partition overwrite replaces the output. |

Each path above gets a test (see section 16).

---

## 14. Cost Controls

Set these up on day one:

- AWS Budget alert (for example a small monthly limit) with email.
- Root account: MFA on, no daily use. Use an IAM Identity Center or IAM user.
- Glue: smallest worker type, 2 workers, short timeout, while developing.
- Athena: workgroup scan limit.
- S3: lifecycle rules for `invalid/`, `athena-results/`, and old versions.
- CloudWatch: log retention set.
- Avoid NAT Gateways and always-on resources.
- Run `cdk destroy` for the dev stack at the end of a session when you are not testing.

---

## 15. Infrastructure and CI/CD

### 15.1 CDK

Build in the console first to learn the services, then codify. Stack boundaries follow dependencies, not one stack per service.

```text
infrastructure/
+-- app.py
+-- stacks/
    +-- storage_stack      (S3, lifecycle, encryption)
    +-- iam_stack          (roles and policies)
    +-- compute_stack      (Lambda, Glue job)
    +-- workflow_stack     (EventBridge, Step Functions, SNS, DLQ)
    +-- monitoring_stack   (alarms, dashboard)
```

### 15.2 GitHub Actions

```text
Push -> Lint -> Unit tests -> CDK synth -> Security checks -> Deploy to dev
```

A second environment can be added later if it is needed.

---

## 16. Testing

- **Unit tests (pytest):** Lambda handler, PySpark transformation functions on small local data.
- **Data tests:** the sample data includes deliberately bad rows, and tests check they land in `invalid/rows`.
- **Failure tests:** one for each path: bad file, bad rows, Glue failure, retries exhausted, duplicate run of the same date.
- **Infrastructure tests:** CDK assertion tests for key resources (bucket encryption, public access blocked).

---

## 17. Repository Structure

```text
commerce-flow/
+-- data/                (sample data + generator script)
+-- docs/
|   +-- architecture.md
|   +-- decisions.md     (one entry per major decision, with the reason)
|   +-- runbook.md       (how to run, re-run, and debug)
+-- infrastructure/
+-- src/
|   +-- lambda/
|   +-- glue/
+-- sql/
+-- tests/
+-- backend/             (Phase 10)
+-- frontend/            (Phase 11)
+-- .github/workflows/
+-- README.md
```

Create folders as the matching component is built.

---

## 18. Phases

The order is chosen so a working analytics result exists early (end of Phase 4), and the Java/React parts are not left until the very end.

| # | Phase | Deliverables | AWS services | Skills practiced |
|---|---|---|---|---|
| 1 | Foundation | Repo, architecture doc, data generator (Python + Faker, 100k+ orders, some bad rows), account safety (MFA, budget alert) | IAM, Budgets | Design, account hygiene |
| 2 | Data lake | S3 zones, naming, upload sample data, encryption, versioning | S3 | Storage design, lifecycle |
| 3 | Glue ETL (console first) | PySpark job, row-level validation, invalid zone, `fact_orders` and aggregates as Parquet, partition overwrite | Glue, S3 | Spark, data quality, idempotency |
| 4 | Catalog + Athena | Glue database and tables, Athena workgroup, business queries | Glue Catalog, Athena | Metadata, SQL, partitioning, scan cost |
| 5 | Validation + trigger | Lambda file checks, S3 EventBridge notification, rule on manifest, DLQ | Lambda, EventBridge, SQS | Event-driven design |
| 6 | Orchestration | Step Functions workflow, retries, failure and success paths, SNS alerts | Step Functions, SNS | Workflow design, error handling |
| 7 | Infrastructure as code | Codify everything in CDK (storage and IAM first) | CDK, CloudFormation | IaC |
| 8 | CI/CD | Lint, tests, synth, deploy with OIDC | GitHub Actions, IAM | Automation |
| 9 | Production readiness | CloudWatch alarms and dashboard, IAM review, failure tests, cost review, runbook | CloudWatch, IAM | Operations, security review |
| 10 | Spring Boot API | Endpoints, Athena via SDK v2, caching, error handling, deployment target decided | API Gateway, Athena, (App Runner / Lambda) | Java backend on AWS |
| 11 | React dashboard | KPI cards, charts, tables, filters, API integration | (S3 + CloudFront or Netlify) | Frontend integration |

**Time guideline:** the core (Phases 1-9) is deep data-engineering work. Cap it. Skip KMS customer-managed keys and extra environments unless needed, so Phases 10 and 11 actually ship. If targeting Java / full-stack roles, those two phases are the ones that match the role.

### Planned API endpoints (Phase 10)

```text
GET /api/customers/top
GET /api/products/top
GET /api/sales/daily
GET /api/customers/{id}/sales
GET /api/categories/performance
```

The API is a thin layer over analytics results. It does not copy the data platform.

### Dashboard areas (Phase 11)

Total revenue, total orders, customer count, daily revenue, top products, top customers, category performance, pipeline health.

---

## 19. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Unexpected AWS cost (Glue, Athena, forgotten resources) | Bill | Budget alert, workgroup limit, small workers, `cdk destroy` |
| Partial file set triggers a run | Wrong output | Manifest-triggered workflow |
| Duplicate output on re-run | Wrong numbers | Partition overwrite, concurrency of 1 |
| Rebuilding everything in CDK takes long | Delay | Start CDK with storage and IAM, move in small steps |
| Core phases use all the time | API and UI never ship | Time cap on Phases 1-9 |
| Athena too slow for the dashboard | Bad UX | Caching or precomputed results |
| Over-permissive IAM during development | Security gap | IAM review in Phase 9, scoped roles from the start |

---

## 20. Definition of Done

The core platform (Phases 1-9) is done when:

- A manifest upload automatically starts processing.
- Raw data is preserved and unchanged.
- Invalid files and rows are detected and stored separately.
- Valid data is transformed with Glue/PySpark into Parquet.
- Re-running the same date gives the same result (no duplicates).
- Tables are in the Glue Catalog and Athena answers the defined business questions.
- Failures are visible (alarms, SNS) and retries are tested.
- IAM is scoped per workload.
- Infrastructure deploys with CDK and CI/CD validates and deploys it.
- The docs explain the architecture, decisions, and how to operate and re-run the pipeline.

The Spring Boot API (Phase 10) and React dashboard (Phase 11) complete the full project.