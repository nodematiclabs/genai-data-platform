import os

from google.cloud import pubsub

# Set your Google Cloud project ID
project_id = os.getenv("GOOGLE_PROJECT")

# Set the desired topic name
topic_name = "orders"

publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

try:
    topic = publisher.create_topic(request={"name": topic_path})
    print(f"Topic created: {topic.name}")
except Exception:
    print("The topic already exists")
