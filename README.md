# F1_ETL_Pipeline

Build an end-to-end ETL pipeline for 2025 OpenF1 data, from extraction through loading into Snowflake.

| Layer                  | Component                                   |
| ---------------------- | ------------------------------------------- |
| **Orchestration**      | Apache Airflow (self-managed or MWAA)       |
| **Extraction**         | Python (`requests`)                         |
| **Transformation**     | Python (`pandas` )          |
| **Loading**            | Snowflake (or your data warehouse)          |
| **Staging & Storage**  | AWS S3 (raw + processed zones)              |
| **Compute**            | AWS Glue (Python shell) or ECS/Fargate pods |
| **Containerization**   | Docker + Docker Compose                     |
| **CI/CD**              | GitHub Actions (or GitLab CI)               |
| **Infrastructure IaC** | Terraform (for S3, IAM, Airflow infra)      |
| **Logging & Metrics**  | CloudWatch Logs       |
| **Secrets & Config**   | AWS Secrets Manager      |
| **Version Control**    | Git                                         |
