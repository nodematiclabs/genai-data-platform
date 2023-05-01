import os
import openai
import time

from google.cloud import bigquery

# Set your Google Cloud project ID and the dataset ID
project_id = os.getenv("GOOGLE_PROJECT")
dataset_id = os.getenv("DATASET")

# Initialize the OpenAI API client
openai.api_key = os.getenv("OPENAI_API_KEY")

def generate_summary(prompt):
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=256,
        n=1,
        stop=None,
        temperature=0.7,
    )

    summary = response.choices[0].text.strip()
    return summary

def update_dataset_description(client, dataset_id, description):
    dataset = client.get_dataset(dataset_id)
    dataset.description = description
    updated_dataset = client.update_dataset(dataset, ["description"])
    print(f"Updated dataset {dataset_id} with description: {updated_dataset.description}")

def get_table_column_names(client, table_id):
    column_names_query = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table_id.split(".")[-1])]
    )

    column_names = [row.column_name for row in client.query(column_names_query, job_config=job_config)]
    return column_names

def update_table_description(client, table_id, description):
    table = client.get_table(table_id)
    table.description = description
    updated_table = client.update_table(table, ["description"])
    print(f"Updated table {table_id} with description: {updated_table.description}")

if __name__ == "__main__":
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)
    tables = [table.table_id for table in client.list_tables(dataset_id)]

    # Update table descriptions based on the OpenAI API-generated summary
    for table in tables:
        table_id = f"{dataset_id}.{table}"
        column_names = get_table_column_names(client, table_id)
        table_prompt = f"Provide a one-sentence use-case-focused description for a {' '.join(table.split('_'))} table with columns '{', '.join(column_names)}'."
        print(table_prompt)
        table_summary = generate_summary(table_prompt)
        update_table_description(client, table_id, table_summary)
        time.sleep(60)

    # Generate and update dataset description using OpenAI API
    dataset_prompt = f"Provide a one-sentence use-case-focused description for a {' '.join(dataset_id.split('_'))} dataset with tables '{', '.join(tables)}'."
    print(dataset_prompt)
    dataset_description = generate_summary(dataset_prompt)
    update_dataset_description(client, dataset_id, dataset_description)