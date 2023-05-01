import pandas as pd
from google.cloud import bigquery

# Replace with your project ID
project_id = os.getenv("GOOGLE_PROJECT")

# Replace with your dataset ID
dataset_id = os.getenv("DATASET")

# Replace with your table name
table_name = "products"

# Replace with the name of the column you want to check for duplicates
column_name = "product_name"

# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Construct the query
query = f"""
WITH deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY {column_name} ORDER BY {column_name}) row_num
  FROM `{project_id}.{dataset_id}.{table_name}`
)
SELECT *
FROM deduped
WHERE row_num = 1;
"""

# Run the query and save the results as a Pandas DataFrame
df = client.query(query).to_dataframe()
df.drop("row_num", axis=1, inplace=True)

# Overwrite the original table with the deduplicated data
destination_table = client.get_table(f"{project_id}.{dataset_id}.{table_name}")
client.load_table_from_dataframe(df, destination_table, location="US").result()

# Print a message to confirm the operation
print(f"The table {table_name} in dataset {dataset_id} has been overwritten with the deduplicated data.")