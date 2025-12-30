from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import requests
import tarfile
import csv
import os

SOURCE_URL = ("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
              "IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz")
DESTINATION_PATH = "/home/project/airflow/dags/python_etl/staging"

default_args = {
    "owner": "alex",
    "start_date": days_ago(0),
    "email": ["test@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule_interval=timedelta(days=1),
)

def download_dataset():
    os.makedirs(DESTINATION_PATH, exist_ok=True)
    response = requests.get(SOURCE_URL, stream=True)
    if response.status_code == 200:
        archive_path = os.path.join(DESTINATION_PATH, "tolldata.tgz")
        with open(archive_path, "wb") as f:
            f.write(response.content)
    else:
        raise Exception(f"Failed to download file: {response.status_code}")

def untar_dataset():
    archive_path = os.path.join(DESTINATION_PATH, "tolldata.tgz")
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=DESTINATION_PATH)

def extract_data_from_csv():
    input_file = os.path.join(DESTINATION_PATH, "vehicle-data.csv")
    output_file = os.path.join(DESTINATION_PATH, "csv_data.csv")
    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type"])
        for row in reader:
            writer.writerow([row[0], row[1], row[2], row[3]])

def extract_data_from_tsv():
    input_file = os.path.join(DESTINATION_PATH, "tollplaza-data.tsv")
    output_file = os.path.join(DESTINATION_PATH, "tsv_data.csv")
    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.reader(infile, delimiter="\t")
        writer = csv.writer(outfile)
        writer.writerow(["Number of axles", "Tollplaza id", "Tollplaza code"])
        for row in reader:
            writer.writerow([row[0], row[1], row[2]])

def extract_data_from_fixed_width():
    input_file = os.path.join(DESTINATION_PATH, "payment-data.txt")
    output_file = os.path.join(DESTINATION_PATH, "fixed_width_data.csv")
    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Type of Payment code", "Vehicle Code"])
        for line in infile:
            pay_code = line[0:6].strip()
            veh_code = line[6:12].strip()
            writer.writerow([pay_code, veh_code])

def consolidate_data():
    csv_file = os.path.join(DESTINATION_PATH, "csv_data.csv")
    tsv_file = os.path.join(DESTINATION_PATH, "tsv_data.csv")
    fixed_file = os.path.join(DESTINATION_PATH, "fixed_width_data.csv")
    output_file = os.path.join(DESTINATION_PATH, "extracted_data.csv")

    with open(csv_file, "r") as csv_in, \
         open(tsv_file, "r") as tsv_in, \
         open(fixed_file, "r") as fixed_in, \
         open(output_file, "w", newline="") as out:

        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out)

        writer.writerow([
            "Rowid",
            "Timestamp",
            "Anonymized Vehicle number",
            "Vehicle type",
            "Number of axles",
            "Tollplaza id",
            "Tollplaza code",
            "Type of Payment code",
            "Vehicle Code",
        ])

        next(csv_reader)
        next(tsv_reader)
        next(fixed_reader)

        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            writer.writerow(csv_row + tsv_row + fixed_row)

def transform_data():
    input_file = os.path.join(DESTINATION_PATH, "extracted_data.csv")
    output_file = os.path.join(DESTINATION_PATH, "transformed_data.csv")

    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row["Vehicle type"] = row["Vehicle type"].upper()
            writer.writerow(row)

download_data = PythonOperator(
    task_id="download_data",
    python_callable=download_dataset,
    dag=dag,
)

unzip_data = PythonOperator(
    task_id="unzip_data",
    python_callable=untar_dataset,
    dag=dag,
)

extract_data_from_csv_task = PythonOperator(
    task_id="extract_data_from_csv",
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_data_from_tsv_task = PythonOperator(
    task_id="extract_data_from_tsv",
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_data_from_fixed_width_task = PythonOperator(
    task_id="extract_data_from_fixed_width",
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_data_task = PythonOperator(
    task_id="consolidate_data",
    python_callable=consolidate_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

download_data >> unzip_data >> [
    extract_data_from_csv_task,
    extract_data_from_tsv_task,
    extract_data_from_fixed_width_task
] >> consolidate_data_task >> transform_data_task
