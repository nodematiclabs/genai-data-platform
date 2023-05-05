# GenAI Data Platform

## Setup

Prerequisite: Create a BigQuery dataset, BigTable instance, Spanner instance, and Cloud Storage bucket based on your cost, security, and region preferences.

Set required environment variables:
```bash
export GOOGLE_PROJECT=""
export REGION=""
export BIGTABLE_INSTANCE=""
export SPANNER_INSTANCE=""
export DATASET=""
export BUCKET=""
export LAKE=""
export ZONE=""
export TAG_TEMPLATE=""
export OPENAI_API_KEY=""
```

Install required Python packages
```bash
pip3 install -r requirements.txt
```

Run resource provisioning
```bash
python3 bigquery-businesses-create.py
python3 bigquery-customers-create.py
python3 bigquery-products-create.py
python3 bigquery-products-dedup.py
python3 bigquery-purchase-orders-create.py
python3 bigtable-iot-create.py
python3 dataplex-resources-create.py
python3 flat-files-create.py
python3 pubsub-orders-create.py
python3 spanner-inventory-create.py
```

Wait for discovery to complete with Dataplex assets.

Run metadata scripts
```bash
python3 bigquery-descriptions-patch.py
python3 dataplex-descriptions-patch.py
python3 dataplex-metadata-attach.py
```