from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector
import sys
import os

# Kafka configuration
TOPIC = os.getenv("KAFKA_TOPIC", "toll")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "toll_consumer_group")

# MySQL configuration
DATABASE = os.getenv("MYSQL_DATABASE", "tolldata")
USERNAME = os.getenv("MYSQL_USER", "etluser")
PASSWORD = os.getenv("MYSQL_PASSWORD", "etlpass123")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

print("Connecting to the database...")

try:
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=DATABASE,
        user=USERNAME,
        password=PASSWORD
    )
except Exception as e:
    print("Could not connect to database")
    print(e)
    sys.exit(1)

print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka...")

try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        value_deserializer=lambda m: m.decode("utf-8"),
    )
except Exception as e:
    print("Could not connect to Kafka")
    print(e)
    sys.exit(1)

print("Connected to Kafka")
print(f"Reading messages from topic '{TOPIC}'")

for msg in consumer:
    try:
        timestamp, vehicle_id, vehicle_type, plaza_id = msg.value.split(",")
        date_obj = datetime.strptime(timestamp, "%a %b %d %H:%M:%S %Y")
        formatted_timestamp = date_obj.strftime("%Y-%m-%d %H:%M:%S")

        sql = """
            INSERT INTO livetolldata (timestamp, vehicle_id, vehicle_type, toll_plaza_id)
            VALUES (%s, %s, %s, %s)
        """

        cursor.execute(
            sql,
            (formatted_timestamp, vehicle_id, vehicle_type, plaza_id)
        )
        connection.commit()

        print(f"Inserted vehicle type '{vehicle_type}' into the database")

    except Exception as e:
        print("Failed to process message:", msg.value)
        print(e)


