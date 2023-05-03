from google.cloud import pubsub_v1

# Set your Google Cloud project ID
project_id = os.getenv("GOOGLE_PROJECT")

# Set the desired topic name
topic_name = "orders"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

try:
    topic = publisher.create_topic(request={"name": topic_path})
    print(f"Topic created: {topic.name}")
except Exception as e:
    print(f"An error occurred: {e}")
