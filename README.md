# Data Engineering Project 1: CSV & API to PostgreSQL ETL

## Overview
This project demonstrates an end-to-end **Data Engineering ETL pipeline** built using **Python and PostgreSQL**.

The pipeline ingests data from:
- A CSV file (batch ingestion)
- A public REST API (JSON response)

The data is transformed and loaded into PostgreSQL tables with proper error handling, logging, and idempotency.

---

##  Tech Stack
- Python
- PostgreSQL
- Pandas
- Requests
- psycopg2
- Git & GitHub

---

##  Project Structure
 data-engineering-project-1/
│
├── data/
│ └── orders.csv
│
├── scripts/
│ ├── csv_to_postgres.py
│ └── api_to_postgres.py
│
├── README.md
├── .gitignore
└── requirements.txt

---

## ETL Flow

### 1. Extract
- CSV data is read using Pandas
- API data is fetched using HTTP GET requests

### 2. Transform
- Date fields are converted to proper datetime format
- Only required fields are selected (data minimization)
- Data types are cleaned before loading

### 3. Load
- Data is loaded into PostgreSQL tables
- Tables are truncated before insert to avoid duplicates
- Transactions are handled safely

---

## Error Handling & Logging
- `try-except-finally` blocks are used
- `response.raise_for_status()` ensures API failures are caught early
- Python `logging` is used instead of print statements
- Database connections are always closed safely

---

## How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt

### 2. Run CSV pipeline
   python scripts/csv_to_postgres.py

###3 Run API pipeline
   python scripts/api_to_postgres.py

Key Learnings

Building production-ready ETL pipelines
Handling API failures gracefully
Preventing duplicate data loads (idempotency)
Writing clean, debuggable Python code
Using Git for version control

Future Improvements
Environment variable based configuration
Data quality checks
Scheduling using Airflow or cron
Containerization using Docker