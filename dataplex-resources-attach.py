import openai
import os
import time

from datetime import datetime

from google.api_core.exceptions import AlreadyExists
from google.cloud import bigquery
from google.cloud import datacatalog
from google.cloud import dataplex

project_id = os.getenv("GOOGLE_PROJECT")
region = os.getenv("REGION")
dataset_id = os.getenv("DATASET")
bigquery_tables = ["customers", "businesses", "products", "purchase_orders"]
tag_template = os.getenv("TAG_TEMPLATE")
entry_group = "logistics"

# Initialize the OpenAI API client
openai.api_key = os.getenv("OPENAI_API_KEY")

bigquery_client = bigquery.Client()
datacatalog_client = datacatalog.DataCatalogClient()
dataplex_scan_client = dataplex.DataScanServiceClient()


def get_table_data(table):
    # Get table headers and rows
    query = f"SELECT * FROM `{dataset_id}.{table}` LIMIT 10"
    query_job = bigquery_client.query(query)
    rows = query_job.result()
    headers = [schema_field.name for schema_field in rows.schema]

    headers_str = ", ".join(headers)
    rows_str = "\n".join([", ".join([str(cell) for cell in row]) for row in rows])
    return headers_str, rows_str


def get_date_range(table):
    request = dataplex.ListDataScanJobsRequest(
        parent=f"projects/{project_id}/locations/{region}/dataScans/{table.replace('_', '-')}-profile",
    )
    # Assumes that the latest job is at index 0 (appears to be the case)
    latest_job_name = [
        job.name for job in dataplex_scan_client.list_data_scan_jobs(request=request)
    ][0]
    request = dataplex.GetDataScanJobRequest(
        name=latest_job_name,
        view="FULL",
    )
    latest_job = dataplex_scan_client.get_data_scan_job(request=request)
    start_date = "1970-01-01T00:00:00Z"
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    for field in latest_job.data_profile_result.profile.fields:
        if field.type_ == "DATE":
            start_date = field.profile.date_profile.min_value
            end_date = field.profile.date_profile.max_value
    return start_date, end_date


def generate_tag_field(prompt):
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=256,
        n=1,
        stop=None,
        temperature=0.7,
    )
    time.sleep(60)  # OpenAI API rate limit

    return response.choices[0].text.strip()


# Data profiling scans
for table in bigquery_tables:
    # Get table headers and rows
    headers_str, rows_str = get_table_data(table)

    # Populate the content tag template with the data profiling scan results
    start_date, end_date = get_date_range(table)
    tag = datacatalog.Tag()
    tag.template = (
        f"projects/{project_id}/locations/{region}/tagTemplates/{tag_template}"
    )
    tag.fields = {
        "source": datacatalog.TagField(
            string_value=generate_tag_field(
                f"What is the most likely data source for a table with headers: {headers_str}. Rows:\n{rows_str}"
            ),
        ),
        "tldr": datacatalog.TagField(
            richtext_value=generate_tag_field(
                f"Provide a one-sentence use-case-focused description for a table with headers: {headers_str}. Rows:\n{rows_str}"
            ),
        ),
        "start_datetime": datacatalog.TagField(
            timestamp_value=start_date,
        ),
        "end_datetime": datacatalog.TagField(
            timestamp_value=end_date,
        ),
        "is_sensitive": datacatalog.TagField(
            bool_value=False
            if (
                generate_tag_field(
                    f'Is there sensitive data in this table?  Respond only "yes" or "no".\n\nTable headers: {headers_str}.\n\nRows:\n{rows_str}'
                )
            )
            == "no"
            else True,
        ),
    }

    # Make the request
    try:
        datacatalog_client.create_entry_group(
            parent=f"projects/{project_id}/locations/{region}",
            entry_group_id=entry_group,
            entry_group=datacatalog.EntryGroup(),
        )
    except AlreadyExists:
        print("Entry group already exists.")

    try:
        entry = datacatalog.Entry()
        entry.display_name = table
        entry.linked_resource = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table}"
        entry.user_specified_system = "bigquery"
        entry.user_specified_type = "table"
        entry.description = generate_tag_field(
            f"Provide a one-sentence use-case-focused description for a table with headers: {headers_str}. Rows:\n{rows_str}"
        )
        try:
            datacatalog_client.delete_entry(
                name=f"projects/{project_id}/locations/{region}/entryGroups/{entry_group}/entries/{table}"
            )
        except:
            pass
        datacatalog_client.create_entry(
            parent=f"projects/{project_id}/locations/{region}/entryGroups/{entry_group}",
            entry_id=table,
            entry=entry,
        )
    except AlreadyExists:
        print("Entry already exists.")
    try:
        datacatalog_client.delete_tag(
            name=f"projects/{project_id}/locations/{region}/entryGroups/{entry_group}/entries/{table}/tags/{tag_template}"
        )
    except:
        pass
    datacatalog_client.create_tag(
        parent=f"projects/{project_id}/locations/{region}/entryGroups/{entry_group}/entries/{table}",
        tag=tag,
    )
