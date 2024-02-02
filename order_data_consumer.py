import json
from google.cloud import pubsub_v1
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
credentials_path=r'C:\Users\Shruti Ghoradkar\Downloads\single-brace-410112-d20286124d0a.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path
# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Project and Topic details
project_id = "single-brace-410112"
subscription_name = "Orders_data-sub"
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def cassandra_connection():
    # Configuration
    CASSANDRA_NODES = ['127.0.0.1']  # Adjust if your Cassandra is hosted elsewhere or in a cluster
    CASSANDRA_PORT = 9042  # Default Cassandra port, adjust if needed
    KEYSPACE = 'ecom_store'
    
    # Connection setup (without authentication)
    cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT)
    
    # Uncomment below lines and adjust USERNAME and PASSWORD if your Cassandra setup requires authentication.
    USERNAME = 'admin'
    PASSWORD = 'admin'
    auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)  
    cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT, auth_provider=auth_provider)
    
    session = cluster.connect(KEYSPACE)

    return cluster,session

# Setup Cassandra connection

cluster,session = cassandra_connection()

# Prepare the Cassandra insertion statement
insert_stmt = session.prepare("""
    INSERT INTO orders_payments_facts (order_id, customer_id, item, quantity, price, shipping_address, order_status, creation_date, payment_id, payment_method, card_last_four, payment_status, payment_datetime)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Pull and process messages
def pull_messages():
    while True:
        response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 10})
        ack_ids = []

        for received_message in response.received_messages:
            # Extract JSON data
            json_data = received_message.message.data.decode('utf-8')
            
            # Deserialize the JSON data
            deserialized_data = json.loads(json_data)

            print(deserialized_data)
            
            # Prepare data for Cassandra insertion
            cassandra_data = (
                deserialized_data.get("order_id"),
                deserialized_data.get("customer_id"),
                deserialized_data.get("item"),
                deserialized_data.get("quantity"),
                deserialized_data.get("price"),
                deserialized_data.get("shipping_address"),
                deserialized_data.get("order_status"),
                deserialized_data.get("creation_date"),
                None,
                None,
                None,
                None,
                None
            )
            
            # Insert data into Cassandra
            session.execute(insert_stmt, cassandra_data)

            print("Data inserted in cassandra !!")
            
            # Collect ack ID for acknowledgment
            ack_ids.append(received_message.ack_id)

        # Acknowledge the messages so they won't be sent again
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

# Run the consumer
if __name__ == "__main__":
    try:
        pull_messages()
    except KeyboardInterrupt:
        pass
    finally:
        # Clean up any resources
        cluster.shutdown()