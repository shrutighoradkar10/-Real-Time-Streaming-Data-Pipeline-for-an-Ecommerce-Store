import json
import time
from google.cloud import pubsub_v1
from random import choice, uniform
import os
credentials_path=r'C:\Users\Shruti Ghoradkar\Downloads\single-brace-410112-d20286124d0a.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path
# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Project and Topic details
project_id = "single-brace-410112"
topic_name = "Payments_data"
topic_path = publisher.topic_path(project_id, topic_name)

# Payment methods mock data
payment_methods = ["Credit Card", "Debit Card", "PayPal", "Google Pay", "Apple Pay"]

def generate_mock_payment(order_id):
    return {
        "payment_id": order_id + 1000,  # Starting from 1001 as in the example
        "order_id": order_id,
        "payment_method": choice(payment_methods),
        "card_last_four": str(order_id).zfill(4)[-4:],  # Just using order_id to make last 4 digits
        "payment_status": "Completed",
        "payment_datetime": f"2024-01-21T{str(order_id).zfill(2)}:01:30Z"  # Using order_id to vary the hour for variety
    }

def callback(future):
    try:
        # Get the message_id after publishing.
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

# Produce mock payment data for each order_id and publish to the topic
for order_id in range(1, 501):  # 500 order_ids
    mock_payment = generate_mock_payment(order_id)
    json_data = json.dumps(mock_payment).encode('utf-8')
    try:
        future = publisher.publish(topic_path, data=json_data)
        future.add_done_callback(callback)
        time.sleep(5)  # Sleep for 2 seconds before producing the next message
    except Exception as e:
        print(f"Exception encountered: {e}")