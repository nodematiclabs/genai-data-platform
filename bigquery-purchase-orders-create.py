import os
from google.cloud import bigquery
from faker import Faker
import random

# Initialize BigQuery client and Faker
client = bigquery.Client()
fake = Faker()

# Set up BigQuery dataset and table
project_id = os.getenv("GOOGLE_PROJECT")
dataset_id = os.getenv("DATASET")
table_id = "purchase_orders"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Create a schema for the table
schema = [
    bigquery.SchemaField("purchase_order_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("business_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("price_per_unit", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("total_cost", "FLOAT", mode="REQUIRED"),
]

# Create the table if it doesn't exist
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table, exists_ok=True)

# Generate and insert fake purchase order data
num_rows = 1000
num_customers = 1000
num_businesses = 1000
rows_to_insert = []
product_names = ["Widget A", "Widget B", "Widget C", "Widget D", "Widget E"]

for i in range(num_rows):
    purchase_order_id = i + 1
    customer_id = random.randint(1, num_customers)
    business_id = random.randint(1, num_businesses)
    order_date = fake.date_between(start_date="-1y", end_date="today")
    product_name = random.choice(product_names)
    quantity = random.randint(1, 100)
    price_per_unit = round(random.uniform(10, 200), 2)
    total_cost = round(quantity * price_per_unit, 2)

    row = (
        purchase_order_id,
        customer_id,
        business_id,
        order_date,
        product_name,
        quantity,
        price_per_unit,
        total_cost,
    )
    rows_to_insert.append(row)

# Insert rows into the table
errors = client.insert_rows(table, rows_to_insert)
if errors == []:
    print(f"Inserted {num_rows} rows successfully.")
else:
    print(f"Encountered errors while inserting rows: {errors}")
