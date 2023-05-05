import os
import time

from google.cloud import bigquery
from google.cloud import datacatalog
from google.cloud import dataplex
from google.cloud import storage

from google.api_core.exceptions import AlreadyExists, Conflict, InvalidArgument

# Set up the necessary variables
project_id = os.getenv("GOOGLE_PROJECT")
region = os.getenv("REGION")
dataset_id = os.getenv("DATASET")
bigquery_tables = ["customers", "businesses", "products", "purchase_orders"]
bucket_name = os.getenv("BUCKET")
lake_id = os.getenv("LAKE")
zone_id = os.getenv("ZONE")
tag_template_id = os.getenv("TAG_TEMPLATE")

# Initialize the BigQuery and Cloud Storage clients
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)
dataplex_service_client = dataplex.DataplexServiceClient()
dataplex_scan_client = dataplex.DataScanServiceClient()
datacatalog_client = datacatalog.DataCatalogClient()

# Create the BigQuery dataset
dataset_ref = bigquery_client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)


# Create the Cloud Storage bucket
try:
    storage_client.create_bucket(bucket_name)
    print(f"Bucket {bucket_name} created in Cloud Storage.")
except Conflict:
    print("Bucket already exists.")

try:
    # Make the data like creation request
    parent = f"projects/{project_id}/locations/{region}"
    operation = dataplex_service_client.create_lake(
        request={
            "parent": parent,
            "lake_id": lake_id,
        }
    )
    print("Waiting for operation to complete...")
    response = operation.result()
    print(response)
except AlreadyExists as e:
    print("Lake already exists.")


# Initialize zone request
parent = f"projects/{project_id}/locations/{region}/lakes/{lake_id}"
zone = dataplex.Zone()
zone.type = "RAW"
zone.resource_spec.location_type = "MULTI_REGION"
request = dataplex.CreateZoneRequest(
    parent=parent,
    zone_id=zone_id,
    zone=zone,
)

# Make the zone request
try:
    operation = dataplex_service_client.create_zone(request=request)
    print("Waiting for operation to complete...")
    response = operation.result()
    print(response)
except InvalidArgument:
    print("Zone already exists.")

# Initialize the dataset asset request
parent = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}"
asset = dataplex.Asset()
asset.resource_spec.name = f"projects/{project_id}/datasets/{dataset_id}"
asset.resource_spec.type = "BIGQUERY_DATASET"
asset.discovery_spec.enabled = True
request = dataplex.CreateAssetRequest(
    parent=parent,
    asset_id="data-platform",
    asset=asset,
)

# Make the dataset asset request
try:
    operation = dataplex_service_client.create_asset(request=request)
    print("Waiting for operation to complete...")
    response = operation.result()
    print(response)
except InvalidArgument:
    print("Asset already exists.")

# Initialize the bucket asset request
parent = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}"
asset = dataplex.Asset()
asset.resource_spec.name = f"projects/{project_id}/buckets/{bucket_name}"
asset.resource_spec.type = "STORAGE_BUCKET"
asset.discovery_spec.enabled = True
asset.discovery_spec.csv_options.header_rows = 1
asset.discovery_spec.include_patterns.append("*.csv")
request = dataplex.CreateAssetRequest(
    parent=parent,
    asset_id="flat-files",
    asset=asset,
)

# Make the bucket asset request
try:
    operation = dataplex_service_client.create_asset(request=request)
    print("Waiting for operation to complete...")
    response = operation.result()
    print(response)
except InvalidArgument:
    print("Asset already exists.")

# Create a tag template request
tag_template = datacatalog.TagTemplate()
tag_template.fields["source"] = datacatalog.TagTemplateField(
    display_name="Source",
    type_={
        "primitive_type": datacatalog.FieldType.PrimitiveType.STRING,
    },
)
tag_template.fields["tldr"] = datacatalog.TagTemplateField(
    display_name="TLDR",
    type_={
        "primitive_type": datacatalog.FieldType.PrimitiveType.RICHTEXT,
    },
)
tag_template.fields["start_datetime"] = datacatalog.TagTemplateField(
    display_name="Start Datetime",
    type_={
        "primitive_type": datacatalog.FieldType.PrimitiveType.TIMESTAMP,
    },
)
tag_template.fields["end_datetime"] = datacatalog.TagTemplateField(
    display_name="End Datetime",
    type_={
        "primitive_type": datacatalog.FieldType.PrimitiveType.TIMESTAMP,
    },
)
tag_template.fields["is_sensitive"] = datacatalog.TagTemplateField(
    display_name="Is Sensitive",
    type_={
        "primitive_type": datacatalog.FieldType.PrimitiveType.BOOL,
    },
)
tag_template.display_name = "context"
request = datacatalog.CreateTagTemplateRequest(
    parent=f"projects/{project_id}/locations/{region}",
    tag_template_id=tag_template_id,
    tag_template=tag_template,
)

# Create the tag template
try:
    response = datacatalog_client.create_tag_template(request=request)
    print(response)
except AlreadyExists:
    print("Tag template already exists.")

# Data profiling scans
for table in bigquery_tables:
    # Create the data profiling scan
    request = dataplex.CreateDataScanRequest(
        parent=f"projects/{project_id}/locations/{region}",
        data_scan_id=f"{table.replace('_', '-')}-profile",
        data_scan=dataplex.DataScan(
            data=dataplex.DataSource(
                entity=f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}/entities/{table}"
            ),
            data_profile_spec=dataplex.DataProfileSpec(),
        ),
    )
    try:
        operation = dataplex_scan_client.create_data_scan(request=request)
        print("Waiting for operation to complete...")
        response = operation.result()
        print(response)
    except AlreadyExists:
        print("Data profile scan already exists.")

    # Run the data profiling scan
    request = dataplex.RunDataScanRequest(
        name=f"projects/{project_id}/locations/{region}/dataScans/{table.replace('_', '-')}-profile",
    )
    response = dataplex_scan_client.run_data_scan(request=request)
    job = response.job
    while job.state in [
        dataplex.DataScanJob.State.PENDING,
        dataplex.DataScanJob.State.RUNNING,
    ]:
        time.sleep(5)
        job = dataplex_scan_client.get_data_scan_job(name=job.name)
        print("Waiting for profile scan to complete...")

    # Get table column names
    table_ref = bigquery_client.dataset(dataset_id, project=project_id).table(table)
    schema = bigquery_client.get_table(table_ref).schema
    column_names = [field.name for field in schema]

    # Create the data quality scan
    request = dataplex.CreateDataScanRequest(
        parent=f"projects/{project_id}/locations/{region}",
        data_scan_id=f"{table.replace('_', '-')}-completeness",
        data_scan=dataplex.DataScan(
            data=dataplex.DataSource(
                entity=f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}/entities/{table}"
            ),
            data_quality_spec=dataplex.DataQualitySpec(
                rules=[
                    dataplex.DataQualityRule(
                        column=column_name,
                        non_null_expectation={},
                        dimension="COMPLETENESS",
                    )
                    for column_name in column_names
                ]
            ),
        ),
    )
    try:
        operation = dataplex_scan_client.create_data_scan(request=request)
        print("Waiting for operation to complete...")
        response = operation.result()
        print(response)
    except AlreadyExists:
        print("Data quality scan already exists.")

    # Run the data quality scan
    request = dataplex.RunDataScanRequest(
        name=f"projects/{project_id}/locations/{region}/dataScans/{table.replace('_', '-')}-completeness",
    )
    response = dataplex_scan_client.run_data_scan(request=request)
    job = response.job
    while job.state in [
        dataplex.DataScanJob.State.PENDING,
        dataplex.DataScanJob.State.RUNNING,
    ]:
        time.sleep(5)
        job = dataplex_scan_client.get_data_scan_job(name=job.name)
        print("Waiting for quality scan to complete...")
