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
table_id = 'customers'
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Create a schema for the table
schema = [
    bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("phone_number", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("zip_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
]

# Create the table if it doesn't exist
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table, exists_ok=True)

# Generate and insert fake customer data
num_rows = 1000
rows_to_insert = []

for i in range(num_rows):
    customer_id = i + 1
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zip_code = fake.zipcode()
    country = fake.country()

    row = (
        customer_id,
        first_name,
        last_name,
        email,
        phone_number,
        address,
        city,
        state,
        zip_code,
        country,
    )
    rows_to_insert.append(row)

# Insert rows into the table
errors = client.insert_rows(table, rows_to_insert)
if errors == []:
    print(f"Inserted {num_rows} rows successfully.")
else:
    print(f"Encountered errors while inserting rows: {errors}")