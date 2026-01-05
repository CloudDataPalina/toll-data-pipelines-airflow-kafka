# 🚦 Toll Data Pipelines (Airflow & Kafka)

![Airflow](https://img.shields.io/badge/Apache-Airflow-blue?logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-Shell_Scripts-121011?logo=gnu-bash&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-Database-orange?logo=mysql&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## ✅ Project Status
This project is fully implemented and tested in a controlled environment.  
It demonstrates **batch and streaming data pipelines** using Apache Airflow and Apache Kafka.
Open for future improvements and enhancements.

---

## 📌 Project Overview

This project implements an end-to-end **data engineering platform** for processing **traffic toll data** using two complementary approaches:

### 🔹 Batch Processing (Apache Airflow)
- Extracts data from multiple file formats:
  - CSV
  - TSV
  - Fixed-width text files
- Transforms and consolidates data into a unified dataset
- Demonstrates two ETL implementations:
  - **BashOperator-based ETL** (shell tools: cut, paste, tr)
  - **PythonOperator-based ETL** (Python, requests, csv)

### 🔹 Streaming Processing (Apache Kafka)
- Consumes real-time vehicle passage events from a Kafka topic
- Inserts streaming data into a MySQL database table
- Demonstrates Kafka → Database ingestion pipeline

This project reflects **real-world data engineering scenarios** where batch and streaming pipelines coexist.

---

## 📂 Project Structure

```text
toll-data-pipelines-airflow-kafka/
├── README.md
├── airflow/
│   └── dags/
│       ├── etl_toll_bashoperator.py      ← Batch ETL using BashOperator
│       └── etl_toll_pythonoperator.py    ← Batch ETL using PythonOperator
│
├── streaming/
│   └── kafka_to_mysql_consumer.py        ← Kafka consumer (streaming ETL)
│
├── sql/
│   └── init_mysql.sql                    ← MySQL schema initialization
│
└── LICENSE
```
---
## 🛠️ Skills & Tools
- Apache Airflow — workflow orchestration and DAG scheduling
- Bash — shell-based ETL processing (cut, paste, tr, tar)
- Python — ETL logic, HTTP requests, CSV parsing
- Apache Kafka — real-time data streaming
- MySQL — persistent storage for streaming data
- Linux CLI — data engineering fundamentals

---

## 🚀 How to Run
### 1️⃣ Batch ETL with Apache Airflow
Two DAGs are available in airflow/dags/:
- etl_toll_bashoperator.py
- etl_toll_pythonoperator.py

#### Steps:
1. Start Apache Airflow
2. Copy DAG files into the `$AIRFLOW_HOME/dags` directory
3. Enable the DAG in the Airflow UI
4. Trigger the DAG manually or via schedule
5. Verify generated files created by the DAG tasks

### 2️⃣ Streaming ETL with Kafka → MySQL
#### Prepare MySQL
Ensure Kafka broker and MySQL are running before starting the consumer.
The consumer runs continuously and processes messages in real time.
Execute the SQL script:

```sql
source sql/init_mysql.sql;
```

This creates:
- database: tolldata
- table: livetolldata

#### Run Kafka Consumer
```bash
python streaming/kafka_to_mysql_consumer.py

```
The consumer:
- reads messages from Kafka topic `toll`
- parses vehicle events
- inserts streaming data into a MySQL database table for persistent storage.

Note: Kafka broker and MySQL are expected to be available on localhost
(e.g. via Docker, local installation, or Codespaces services).
Kafka and MySQL were validated using Docker Compose in GitHub Codespaces (local development setup).

---

## ✅ Example Output (Streaming Verification)

After producing a test message to the Kafka topic and running the consumer,
the data is successfully inserted into the MySQL database.

#### Sample Kafka Message
```text
Sat Jun 01 12:00:00 2024,101,car,7
```

#### MySQL Table Output
```sql
SELECT * FROM livetolldata;
```

```text
+---------------------+------------+--------------+----------------+
| timestamp           | vehicle_id | vehicle_type | toll_plaza_id |
+---------------------+------------+--------------+----------------+
| 2024-06-01 12:00:00 | 101        | car          | 7              |
+---------------------+------------+--------------+----------------+
```
--- 

## 🔄 Data Flow Diagram
```text
                ┌──────────────┐
                │ CSV / TSV /  │
                │ Fixed-width  │
                │   files      │
                └──────┬───────┘
                       │
                 Apache Airflow
                       │
           ┌───────────┴───────────┐
           │                       │
   BashOperator ETL        PythonOperator ETL
           │                       │
           └───────────┬───────────┘
                       │
              transformed_data.csv
                       │
        ─────────────────────────────────
                       │
                   Kafka Topic
                       │
              Kafka Consumer (Python)
                       │
                    MySQL DB
                 livetolldata table
```
## 🔎 Key Engineering Highlights
- Same ETL logic implemented using **two different orchestration styles**
- Demonstrates **batch vs streaming** processing
- Clear separation of concerns (Airflow / Kafka / SQL)
- Production-oriented project structure
- Readable, maintainable, and extensible codebase

## 📝 Summary
- Built batch ETL pipelines with Apache Airflow
- Implemented streaming ingestion with Apache Kafka
- Stored real-time data into MySQL
- Demonstrated core Data Engineering concepts end-to-end

## 📜 License

This project is released under the **MIT License** — see [`LICENSE`](LICENSE).  

***Enjoy and have a great data brew*** ☕️🙂

---

## 👩‍💻 Author

**Palina Krasiuk**  
Aspiring Cloud Data Engineer | ex-Senior Accountant  
[LinkedIn](https://www.linkedin.com/in/palina-krasiuk-954404372/) • [GitHub Portfolio](https://github.com/CloudDataPalina)


  
