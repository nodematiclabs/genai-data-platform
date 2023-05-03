import csv
import random
import os
from faker import Faker
from google.cloud import storage

fake = Faker()

# Constants
NUM_SUPPLIERS = 100
CSV_FILE = "suppliers.csv"
GCS_BUCKET_NAME = os.getenv("BUCKET")


def generate_supplier_data():
    return {
        "supplier_id": fake.uuid4(),
        "company_name": fake.company(),
        "contact_name": fake.name(),
        "contact_title": fake.job(),
        "address": fake.address().replace("\n", ", "),
        "city": fake.city(),
        "region": fake.state(),
        "postal_code": fake.zipcode(),
        "country": fake.country(),
        "phone": fake.phone_number(),
        "email": fake.company_email(),
        "website": fake.url(),
        "num_employees": random.randint(50, 1000),
    }


def write_csv_file(filename, data):
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    with open(source_file, "rb") as f:
        blob.upload_from_file(f)

    print(f"File {source_file} uploaded to {destination_blob_name}.")


if __name__ == "__main__":
    suppliers_data = [generate_supplier_data() for _ in range(NUM_SUPPLIERS)]
    write_csv_file(CSV_FILE, suppliers_data)
    print(f"Generated {CSV_FILE} with {NUM_SUPPLIERS} fake suppliers.")

    # Upload the CSV file to Google Cloud Storage
    upload_to_gcs(GCS_BUCKET_NAME, CSV_FILE, CSV_FILE)
    print(f"Uploaded {CSV_FILE} to Google Cloud Storage bucket {GCS_BUCKET_NAME}.")
