from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector
import sys

# Kafka configuration
TOPIC = "toll"
BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP = "toll_consumer_group"

# MySQL configuration
DATABASE = "tolldata"
USERNAME = "etluser"
PASSWORD = "etlpass123"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306

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

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="latest"
)

print("Connected to Kafka")
print(f"Reading messages from topic '{TOPIC}'")

for msg in consumer:
    message = msg.value.decode("utf-8")
    timestamp, vehicle_id, vehicle_type, plaza_id = message.split(",")

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



