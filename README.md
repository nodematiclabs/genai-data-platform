# GenAI Data Platform

Create a BigQuery dataset, BigTable instance, Spanner instance, and Cloud Storage bucket based on your cost, security, and region preferences.

Set required environment variables:
```bash
export GOOGLE_PROJECT=""
export REGION=""
export BIGTABLE_INSTANCE=""
export SPANNER_INSTANCE=""
export DATASET=""
export BUCKET=""
export OPENAI_API_KEY=""
```

```bash
pip install google-cloud-bigquery
pip install google-cloud-bigtable
pip install google-cloud-datacatalog
pip install google-cloud-dataplex
pip install google-cloud-pubsub
pip install google-cloud-spanner
pip install Faker
```