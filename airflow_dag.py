from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'car_rental_data_pipeline',
    default_args=default_args,
    description='Car Rental Data Pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 8, 2),
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    }
)

# Python function to get the execution date
def get_execution_date(ds_nodash, **kwargs):
    print(kwargs)
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# PythonOperator to call the get_execution_date function
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# Task to perform SCD2 merge on customer_dim
merge_customer_dim = SnowflakeOperator(
    task_id='merge_customer_dim',
    snowflake_conn_id='snowflake_conn',
    sql="""
        MERGE INTO car_rental.public.customer_dim AS target
        USING (
            SELECT
                $1 AS customer_id,
                $2 AS name,
                $3 AS email,
                $4 AS phone
            FROM @car_rental.public.car_rental_data_stage/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv (FILE_FORMAT => 'csv_format')
        ) AS source
        ON target.customer_id = source.customer_id AND target.is_current = TRUE
        WHEN MATCHED AND (
            target.name != source.name OR
            target.email != source.email OR
            target.phone != source.phone
        ) THEN
            UPDATE SET target.end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE
        WHEN NOT MATCHED THEN 
        INSERT (customer_id, name, email, phone, effective_date, end_date, is_current)
        VALUES (source.customer_id, source.name , source.email, source.phone, CURRENT_TIMESTAMP(), NULL, TRUE);
    """,
    dag=dag,
)

# Replace it with spark job python script path / repository url
pyspark_job_file_path = './dags/spark_job.py'

submit_pyspark_job = SparkSubmitOperator(
    application=pyspark_job_file_path,
    task_id="submit_pyspark_job",
    conn_id='spark_conn',
    application_args =['--date={{ ti.xcom_pull(task_ids=\'get_execution_date\') }}'],
    dag=dag
)

# Set the task dependencies
get_execution_date_task >> merge_customer_dim >> submit_pyspark_job
