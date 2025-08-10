# F1_ETL_Pipeline

Built an end-to-end ETL pipeline for 2025 OpenF1 data, from extraction through loading into Postgres using Apache Airflow.

<img width="633" height="448" alt="image" src="https://github.com/user-attachments/assets/ae371c7b-782e-4686-8172-1f233dcf387c" />


| Layer                  | Component                                   |
| ---------------------- | ------------------------------------------- |
| **Orchestration**      | Apache Airflow                              |
| **Extraction**         | Python (`requests`)                         |
| **Transformation**     | Python (`SQLAlchemy` )                      |
| **Loading**            | Posstgres                                   |
| **Staging & Storage**  | AWS S3 (raw + processed zones)              |
| **Containerization**   | Docker + Docker Compose                     |
| **CI/CD**              | GitHub Actions (or GitLab CI)               |

