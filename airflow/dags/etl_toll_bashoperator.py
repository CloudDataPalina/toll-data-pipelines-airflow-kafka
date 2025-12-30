from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Default DAG arguments
default_args = {
    "owner": "Palina",
    "start_date": datetime(2025, 12, 20),
    "email": ["palina@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="ETL_toll_data_bash",
    default_args=default_args,
    description="Batch ETL pipeline using BashOperator",
    schedule_interval="@daily",
)

BASE_PATH = "/home/project/airflow/dags/finalassignment"

unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command=(
        f"tar -xvzf {BASE_PATH}/tolldata.tgz "
        f"-C {BASE_PATH}"
    ),
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command=(
        f"cut -d',' -f1-4 {BASE_PATH}/vehicle-data.csv "
        f"> {BASE_PATH}/csv_data.csv"
    ),
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command=(
        f"cut -d$'\\t' -f1-3 {BASE_PATH}/tollplaza-data.tsv "
        f"> {BASE_PATH}/tsv_data.csv"
    ),
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command=(
        f"cut -c1-6,7-14 {BASE_PATH}/payment-data.txt "
        f"> {BASE_PATH}/fixed_width_data.csv"
    ),
    dag=dag,
)

consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command=(
        f"paste -d',' "
        f"{BASE_PATH}/csv_data.csv "
        f"{BASE_PATH}/tsv_data.csv "
        f"{BASE_PATH}/fixed_width_data.csv "
        f"> {BASE_PATH}/extracted_data.csv"
    ),
    dag=dag,
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command=(
        f"head -n 1 {BASE_PATH}/extracted_data.csv > "
        f"{BASE_PATH}/transformed_data.csv && "
        f"tail -n +2 {BASE_PATH}/extracted_data.csv | "
        f"tr '[:lower:]' '[:upper:]' >> "
        f"{BASE_PATH}/transformed_data.csv"
    ),
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> \
extract_data_from_fixed_width >> consolidate_data >> transform_data


