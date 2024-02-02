import json
import time
import random
import os
from google.cloud import pubsub_v1

# Initialize the Pub/Sub publisher client
credentials_path=r'C:\Users\Shruti Ghoradkar\Downloads\single-brace-410112-d20286124d0a.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path
publisher = pubsub_v1.PublisherClient()

# Project and Topic details
project_id = "single-brace-410112"
topic_name = "Orders_data"
topic_path = publisher.topic_path(project_id, topic_name)

# Callback function to handle the publishing results.
def callback(future):
    try:
        # Get the message_id after publishing.
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

def generate_mock_data(order_id):
    items = ["Laptop", "Phone", "Book", "Tablet", "Monitor"]
    addresses = ["123 Main St, City A, Country", "456 Elm St, City B, Country", "789 Oak St, City C, Country"]
    statuses = ["Shipped", "Pending", "Delivered", "Cancelled"]

    return {
        "order_id": order_id,
        "customer_id": random.randint(100, 1000),
        "item": random.choice(items),
        "quantity": random.randint(1, 10),
        "price": random.uniform(100, 1500),
        "shipping_address": random.choice(addresses),
        "order_status": random.choice(statuses),
        "creation_date": "2024-01-21"
    }

order_id = 1
while True:
    data = generate_mock_data(order_id)
    json_data = json.dumps(data).encode('utf-8')

    try:
        future = publisher.publish(topic_path, data=json_data)
        future.add_done_callback(callback)
        future.result()
    except Exception as e:
        print(f"Exception encountered: {e}")

    time.sleep(5)  # Wait for 2 seconds

    order_id += 1
    if order_id > 80:
        order_id = 1  # Reset the order_id back to 1 after reaching 500