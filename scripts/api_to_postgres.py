import requests
import pandas as pd
import psycopg2
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

try:
    logging.info("Starting API ETL pipeline")

    # 1. Extract
    url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    logging.info("Data extracted from API")

    # Convert JSON to DataFrame
    df = pd.DataFrame(data)

    # 2. Transform
    df = df[["country", "cases", "deaths", "recovered", "updated"]]

    df["last_updated"] = df["updated"].apply(
        lambda x: datetime.fromtimestamp(x / 1000)
    )

    df.drop(columns=["updated"], inplace=True)

    logging.info("Data transformation completed")

    # 3. Load
    conn = psycopg2.connect(
        host="localhost",
        database="de_project_1",
        user="postgres",
        password="Veena@9618"
    )

    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE covid_stats;")

    insert_query = """
    INSERT INTO covid_stats (country, cases, deaths, recovered, last_updated)
    VALUES (%s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cur.execute(insert_query, tuple(row))

    conn.commit()

    logging.info("Data successfully loaded into PostgreSQL")

except Exception as e:
    logging.error("ETL pipeline failed", exc_info=True)

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()

    logging.info("Database connection closed")
