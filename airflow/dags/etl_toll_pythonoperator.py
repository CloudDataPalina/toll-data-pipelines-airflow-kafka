from __future__ import annotations

from datetime import datetime, timedelta
import csv
import os
import tarfile

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

SOURCE_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
)
STAGING_DIR = "/home/project/airflow/dags/python_etl/staging"
ARCHIVE_PATH = os.path.join(STAGING_DIR, "tolldata.tgz")


default_args = {
    "owner": "Palina",
    "email": ["palina@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def download_dataset() -> None:
    os.makedirs(STAGING_DIR, exist_ok=True)
    with requests.get(SOURCE_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(ARCHIVE_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def untar_dataset() -> None:
    with tarfile.open(ARCHIVE_PATH, "r:gz") as tar:
        tar.extractall(path=STAGING_DIR)


def extract_data_from_csv() -> None:
    input_file = os.path.join(STAGING_DIR, "vehicle-data.csv")
    output_file = os.path.join(STAGING_DIR, "csv_data.csv")

    with open(input_file, "r", newline="") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            if not row:
                continue
            writer.writerow(row[0:4])


def extract_data_from_tsv() -> None:
    input_file = os.path.join(STAGING_DIR, "tollplaza-data.tsv")
    output_file = os.path.join(STAGING_DIR, "tsv_data.csv")

    with open(input_file, "r", newline="") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.reader(infile, delimiter="\t")
        writer = csv.writer(outfile)

        for row in reader:
            if not row:
                continue
            writer.writerow(row[0:3])


def extract_data_from_fixed_width() -> None:
    input_file = os.path.join(STAGING_DIR, "payment-data.txt")
    output_file = os.path.join(STAGING_DIR, "fixed_width_data.csv")

    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        writer = csv.writer(outfile)

        for line in infile:
            if not line.strip():
                continue
            payment_code = line[0:6].strip()
            vehicle_code = line[6:14].strip()
            writer.writerow([payment_code, vehicle_code])


def consolidate_data() -> None:
    csv_file = os.path.join(STAGING_DIR, "csv_data.csv")
    tsv_file = os.path.join(STAGING_DIR, "tsv_data.csv")
    fixed_file = os.path.join(STAGING_DIR, "fixed_width_data.csv")
    output_file = os.path.join(STAGING_DIR, "extracted_data.csv")

    with open(csv_file, "r", newline="") as csv_in, open(tsv_file, "r", newline="") as tsv_in, open(
        fixed_file, "r", newline=""
    ) as fixed_in, open(output_file, "w", newline="") as out:
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out)

        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            writer.writerow(csv_row + tsv_row + fixed_row)


def transform_data() -> None:
    input_file = os.path.join(STAGING_DIR, "extracted_data.csv")
    output_file = os.path.join(STAGING_DIR, "transformed_data.csv")

    with open(input_file, "r", newline="") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            if not row:
                continue
            if len(row) >= 4:
                row[3] = row[3].upper()
            writer.writerow(row)


with DAG(
    dag_id="ETL_toll_data_python",
    default_args=default_args,
    description="Toll data ETL (PythonOperator)",
    start_date=datetime(2025, 12, 20),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    download_data = PythonOperator(task_id="download_data", python_callable=download_dataset)
    unzip_data = PythonOperator(task_id="unzip_data", python_callable=untar_dataset)

    extract_csv = PythonOperator(task_id="extract_data_from_csv", python_callable=extract_data_from_csv)
    extract_tsv = PythonOperator(task_id="extract_data_from_tsv", python_callable=extract_data_from_tsv)
    extract_fixed = PythonOperator(
        task_id="extract_data_from_fixed_width", python_callable=extract_data_from_fixed_width
    )

    consolidate = PythonOperator(task_id="consolidate_data", python_callable=consolidate_data)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_data)

    download_data >> unzip_data >> [extract_csv, extract_tsv, extract_fixed] >> consolidate >> transform
