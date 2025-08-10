# F1_ETL_Pipeline

Build an end-to-end ETL pipeline for 2025 OpenF1 data, from extraction through loading into Postgres using Apache Airflow.

| Layer                  | Component                                   |
| ---------------------- | ------------------------------------------- |
| **Orchestration**      | Apache Airflow                              |
| **Extraction**         | Python (`requests`)                         |
| **Transformation**     | Python (`SQLAlchemy` )                      |
| **Loading**            | Posstgres                                   |
| **Staging & Storage**  | AWS S3 (raw + processed zones)              |
| **Containerization**   | Docker + Docker Compose                     |
| **CI/CD**              | GitHub Actions (or GitLab CI)               |

