import os
import random
import string

from faker import Faker
from google.cloud import spanner

# Set up Google Cloud Spanner client
project_id = os.getenv("GOOGLE_PROJECT")
instance_id = os.getenv("SPANNER_INSTANCE")
database_id = "inventory"

# The number of rows to generate
num_rows = 100

spanner_client = spanner.Client(project=project_id)
instance = spanner_client.instance(instance_id)

# Create a Cloud Spanner database
def create_database():
    database = instance.database(database_id, ddl_statements=[
        """CREATE TABLE Inventory (
            ItemID STRING(36) NOT NULL,
            ItemName STRING(256) NOT NULL,
            Warehouse STRING(256) NOT NULL,
            Quantity INT64 NOT NULL,
            UnitCost FLOAT64 NOT NULL,
        ) PRIMARY KEY (ItemID)"""
    ])
    operation = database.create()
    print("Waiting for operation to complete...")
    operation.result()
    print("Database created.")

# Generate random, fake manufacturing inventory data
def generate_fake_data():
    fake = Faker()
    data = []
    for _ in range(num_rows):  # Generate records
        item_id = fake.uuid4()
        item_name = f"Widget {random.choice(string.ascii_uppercase)}"
        warehouse = fake.city()
        quantity = random.randint(1, 100)
        unit_cost = round(random.uniform(10, 1000), 2)
        data.append((item_id, item_name, warehouse, quantity, unit_cost))
    return data

# Insert fake data into the database
def insert_fake_data(database, data):
    def insert_data(transaction):
        transaction.insert(
            "Inventory",
            columns=("ItemID", "ItemName", "Warehouse", "Quantity", "UnitCost"),
            values=data
        )
    database.run_in_transaction(insert_data)
    print(f"Inserted {len(data)} records.")

create_database()
database = instance.database(database_id)
fake_data = generate_fake_data()
insert_fake_data(database, fake_data)