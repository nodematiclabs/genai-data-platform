import os
from google.cloud import bigquery
from faker import Faker
import random
import string

# Initialize BigQuery client and Faker
client = bigquery.Client()
fake = Faker()

# Set up BigQuery dataset and table
project_id = os.getenv("GOOGLE_PROJECT")
dataset_id = os.getenv("DATASET")
table_id = 'products'
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Create a schema for the table
schema = [
    bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dimensions", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("weight", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("materials", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("color", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("manufacturing_date", "DATE", mode="REQUIRED"),
]

# Create the table if it doesn't exist
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table, exists_ok=True)

# Generate and insert fake product topology and metadata
num_rows = 1000
rows_to_insert = []
categories = ["Electronics", "Furniture", "Tools", "Automotive", "Toys", "Appliances", "Clothing", "Accessories"]
materials = ["Plastic", "Wood", "Metal", "Ceramic", "Glass", "Fabric", "Composite"]
colors = ["Red", "Blue", "Green", "Yellow", "Black", "White", "Silver", "Gold", "Orange", "Purple"]

for i in range(num_rows):
    product_id = i + 1
    product_name = f"Widget {random.choice(string.ascii_uppercase)}"
    category = random.choice(categories)
    dimensions = f"{random.randint(1, 50)}x{random.randint(1, 50)}x{random.randint(1, 50)} cm"
    weight = round(random.uniform(0.5, 50), 2)
    material = random.choice(materials)
    color = random.choice(colors)
    manufacturing_date = fake.date_between(start_date="-5y", end_date="today")

    row = (
        product_id,
        product_name,
        category,
        dimensions,
        weight,
        material,
        color,
        manufacturing_date,
    )
    rows_to_insert.append(row)

# Insert rows into the table
errors = client.insert_rows(table, rows_to_insert)
if errors == []:
    print(f"Inserted {num_rows} rows successfully.")
else:
    print(f"Encountered errors while inserting rows: {errors}")