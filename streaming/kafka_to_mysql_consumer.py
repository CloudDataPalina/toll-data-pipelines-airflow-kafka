from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector
import sys

TOPIC = 'toll'

DATABASE = 'tolldata'
USERNAME = 'etluser'
PASSWORD = 'etlpass123'
MYSQL_HOST = '172.21.31.203'
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

print("Connecting to Kafka")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092'
)

print("Connected to Kafka")
print(f"Reading messages from topic {TOPIC}")

for msg in consumer:
    message = msg.value.decode("utf-8")
    (timestamp, vehicle_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, "%a %b %d %H:%M:%S %Y")
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    sql = "INSERT INTO livetolldata VALUES (%s,%s,%s,%s)"
    cursor.execute(sql, (timestamp, vehicle_id, vehicle_type, plaza_id))
    connection.commit()

    print(f"A {vehicle_type} was inserted into the database")

