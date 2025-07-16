# !pip install google-cloud-pubsub
import json
import uuid
import random
from datetime import datetime, timezone

# Import the Google Cloud Pub/Sub client library
from google.cloud import pubsub_v1

# --- Configuration for Pub/Sub ---
# Replace with your actual Google Cloud Project ID
PROJECT_ID = "quiet-pagoda-465017-d5"  # Replace with your GCP Project ID
TOPIC_ID = "topic-test"

# --- Sample Data for Simulation ---
PRODUCTS = {
    "PROD001": {"name": "Laptop Pro X", "category": "Electronics"},
    "PROD002": {"name": "Ergonomic Keyboard", "category": "Electronics"},
    "PROD003": {"name": "Wireless Mouse", "category": "Electronics"},
    "PROD004": {"name": "Coffee Mug", "category": "Homeware"},
    "PROD005": {"name": "Bluetooth Speaker", "category": "Electronics"},
    "PROD006": {"name": "Gaming Headset", "category": "Electronics"},
    "PROD007": {"name": "Yoga Mat", "category": "Fitness"},
    "PROD008": {"name": "Water Bottle", "category": "Fitness"},
    "PROD009": {"name": "Sneakers", "category": "Apparel"},
    "PROD010": {"name": "T-Shirt", "category": "Apparel"},
}
STORE_IDS = ["STR001", "STR002", "STR003", "STR004", "STR005"]
CUSTOMER_IDS = [f"CUST{i:03d}" for i in range(1, 21)] # 20 sample customers

def generate_sales_transaction():
    """
    Generates a single sales transaction dictionary with random values.
    """
    product_id = random.choice(list(PRODUCTS.keys()))
    quantity = random.randint(1, 5)
    price_per_unit = round(random.uniform(5.00, 500.00), 2)
    store_id = random.choice(STORE_IDS)
    customer_id = random.choice(CUSTOMER_IDS) if random.random() > 0.1 else None

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "product_id": product_id,
        "quantity": quantity,
        "price_per_unit": price_per_unit,
        "store_id": store_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds') + 'Z',
        "customer_id": customer_id
    }
    return transaction

def publish_message_to_pubsub(project_id: str, topic_id: str, message: dict):
    """
    Publishes a dictionary message as a JSON string to a Pub/Sub topic.

    Args:
        project_id: The ID of your Google Cloud Project.
        topic_id: The name of your Pub/Sub topic.
        message: The dictionary containing the message data to publish.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # Convert the dictionary message to a JSON string, then encode to bytes.
    # Pub/Sub messages must be bytes.
    data = json.dumps(message).encode("utf-8")

    try:
        # Publish the message
        future = publisher.publish(topic_path, data)
        # The .result() method blocks until the message is published.
        # It returns the message ID, which confirms successful publication.
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"An error occurred during publishing: {e}")

# --- Main execution ---
if __name__ == "__main__":
    # 1. Generate the transaction message
    for i in range(1,40):
      transaction_data = generate_sales_transaction()
      # print("Generated transaction:")
      # print(json.dumps(transaction_data, indent=2))

    # 2. Publish the generated message to Pub/Sub
    # IMPORTANT: Replace "your-gcp-project-id" and "your-pubsub-topic-name"
    # with your actual Google Cloud Project ID and Pub/Sub Topic ID.
      publish_message_to_pubsub(PROJECT_ID, TOPIC_ID, transaction_data)
