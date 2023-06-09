import os
import openai
import time

from google.cloud import dataplex

# Set your Google Cloud project ID, Dataplex zone ID, and Dataplex lake ID
project_id = os.getenv("GOOGLE_PROJECT")
region = os.getenv("REGION")
lake_id = os.getenv("LAKE")
zone_id = os.getenv("ZONE")

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
    time.sleep(60)  # OpenAI rate limiting 

    summary = response.choices[0].text.strip()
    return summary


def update_dataplex_asset_description(dataplex_client, asset_id, description):
    asset = dataplex_client.get_asset(name=asset_id)
    asset.description = description
    operation = dataplex_client.update_asset(
        asset=asset, update_mask={"paths": ["description"]}
    )
    result = operation.result()
    print(f"Updated Dataplex asset {asset_id} with description: {result.description}")


def update_dataplex_entity_description(dataplex_client, entity_id, description):
    entity = dataplex_client.get_entity(name=entity_id)
    entity.description = description
    request = dataplex.UpdateEntityRequest(
        entity=entity,
    )
    entity = dataplex_client.update_entity(request=request)
    print(f"Updated Dataplex entity {entity_id} with description: {entity.description}")


if __name__ == "__main__":
    # Create a Dataplex client
    dataplex_service_client = dataplex.DataplexServiceClient()
    dataplex_metadata_client = dataplex.MetadataServiceClient()

    # Generate and update Dataplex asset descriptions using OpenAI API
    for asset in dataplex_service_client.list_assets(
        parent=f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}"
    ):
        asset_id = asset.name
        asset_prompt = f"Provide a one-sentence use-case-focused description for a Dataplex asset with the ID '{asset_id}'."
        asset_description = generate_summary(asset_prompt)
        update_dataplex_asset_description(
            dataplex_service_client, asset_id, asset_description
        )
