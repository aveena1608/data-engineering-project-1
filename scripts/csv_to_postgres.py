import pandas as pd
import psycopg2

# 1. Extract
df = pd.read_csv("../data/orders.csv")

# 2. Transform
df["order_date"] = pd.to_datetime(df["order_date"])
df["total_amount"] = df["quantity"] * df["price"]

# 3. Load
conn = psycopg2.connect(
    host="localhost",
    database="de_project_1",
    user="postgres",
    password="Veena@9618"
)

cur = conn.cursor()

insert_query = """
INSERT INTO orders (order_id, order_date, customer_id, product, quantity, price, total_amount)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

for _, row in df.iterrows():
    cur.execute(insert_query, tuple(row))

conn.commit()
cur.close()
conn.close()

print("CSV data loaded successfully!")
