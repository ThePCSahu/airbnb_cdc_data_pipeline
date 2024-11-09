# Car Rental Batch Data Ingestion Pipeline with SCD2 Merge

## Tech Stack:
- **Python**
- **PySpark**
- **Airflow**
- **Snowflake**
- **Hadoop**
- **Azure Data Lake Storage**

## Overview:
This pipeline is designed to ingest and process car rental data using batch processing and Slowly Changing Dimension (SCD) Type 2 (SCD2) merge strategy. The pipeline consists of two main components:
1. **Airflow Orchestration** - Coordinates the execution of the pipeline tasks.
2. **Spark Job** - Processes the car rental data, performs necessary transformations, and loads it into Snowflake.

### Airflow Tasks:
Airflow is used to orchestrate the following tasks:

1. **Get Execution Date**: Fetch the execution date for the batch to be processed.
2. **SCD2 Merge**: 
    - Reads daily customer raw data from **Azure Data Lake Storage** using a **Snowflake External Stage**.
    - Performs SCD2 merge to update the **customer_dim** table in Snowflake.
3. **Submit Spark Job**: 
    - Submits a Spark job to the cluster to perform additional operations.

### Spark Job Tasks:
The Spark job performs the following tasks:
1. **Read Dimension Tables**: Reads the static **location_dim**, **date_dim**, and **car_dim** tables and **customer_dim** scd table from Snowflake.
2. **Read and Clean Data**: Reads the daily car rental raw data from HDFS, applies necessary data cleaning steps.
3. **Transform Data**: Joins the cleaned rental data with dimension data and applies any required transformations.
4. **Load to Snowflake**: Saves the transformed data into the **rentals_fact** table in Snowflake.

## System Diagram
This diagram illustrates the architecture and data flow of the car rental data pipeline, showing the key components and their interactions.

```
+--------------------------+
|   Airflow Orchestration  |
+--------------------------+
            |
            v
+--------------------------+
|    Get Execution Date    |
+--------------------------+
            |
            v
+--------------------------+      +------------------------------+
|      SCD2 Merge          |      |  Azure Data Lake Storage     |
| - Reads customer data    | ---> | (External Stage in Snowflake)|
| - Updates customer_dim   |      +------------------------------+
+--------------------------+
            |
            v
+--------------------------+
|   Submit Spark Job       |
+--------------------------+
            |
            v
+--------------------------+
|   Spark Job (Cluster)    |
+--------------------------+
            |
            v
+--------------------------+
|   Read & Clean Raw       |
| - car rental data        |
| - Apply cleaning         |
+--------------------------+
            |
            v
+--------------------------+   
|   Read Dimension Data    |   
| - location_dim           |   
| - date_dim               |   
| - car_dim                |   
| - customer_dim (SCD2)    |
+--------------------------+
            |
            v
+--------------------------+
|     Transform Data       |
| - Join with dimensions   |
| - Apply transformations  |
+--------------------------+
            |
            v
+--------------------------+
|    Load to Snowflake     |
| - rentals_fact table     |
+--------------------------+
            |
            v
+--------------------------+
|   Snowflake Data Store   |
+--------------------------+
```

## Entity-Relationship Diagram (ERD)
Representation of the car rental data model and its relationships.

```
                                  +--------------------+
                                  |  date_dim (static) |
                                  +--------------------+
                                  | date_key (PK) INT  |
                                  | date          DATE |
                                  | year          INT  |
                                  | month         INT  |
                                  | day           INT  |
                                  | quarter       INT  |
                                  +--------------------+
                                            ^
                                            |
                                            |
+-----------------------+     +----------------------------+     +------------------------+
| location_dim (static) |     |        rentals_fact        |     |    car_dim (static)    |
+-----------------------+     +----------------------------+     +------------------------+
| location_key (PK) INT |<----| rental_id (PK)       STR   |---->| car_key (FK)     INT   |
| location_id (UQ)  STR |     | customer_key (FK)    INT   |     | car_id (UQ)      STR   |
| location_name     STR |     | car_key (FK)         INT   |     | make             STR   |
+-----------------------+     | pickup_loc_key (FK)  INT   |     | model            STR   |
                              | dropoff_loc_key (FK) INT   |     | year             INT   |
                              | start_date_key (FK)  INT   |     +------------------------+
                              | end_date_key (FK)    INT   |
                              | amount               FLOAT |
                              | quantity             INT   |
                              | rental_dur_days      INT   |
                              | total_rental_amt     FLOAT |
                              | avg_daily_rent_amt   FLOAT |
                              | is_long_rental       BOOL  |
                              +----------------------------+
                                           |
                                           v
                              +-----------------------------+
                              |     customer_dim (SCD2)     |
                              +-----------------------------+
                              | customer_key (PK)  INT      |
                              | customer_id (UQ)   STR      |
                              | name               STR      |
                              | email              STR      |
                              | phone              STR      |
                              | effective_date     TIMESTAMP|
                              | end_date           TIMESTAMP|
                              | is_current         BOOL     |
                              +-----------------------------+

```

## Setup Steps:

### Step 1: Set up Azure Data Lake Storage
1. Log in to your **Azure** account and set up **Azure Data Lake Storage**.
2. Obtain the **SAS Token** for secure access to the storage.
3. Create the required directory and upload the **customer raw data** to the directory.

### Step 2: Snowflake Setup
1. Log in to your **Snowflake** account.
2. Execute the `snowflake_dwh_setup.sql` file to create the necessary tables and external stage.
3. Use the SAS token and URL to configure the external stage for reading data from Azure Data Lake.

### Step 3: Airflow Setup
1. Log in to your **Airflow** account.
2. Ensure the following Python packages are installed:
   - `apache-airflow-providers-apache-spark`
   - `apache-airflow-providers-snowflake`
3. Verify that the **JAVA_HOME** environment variable is set in Airflow.
4. Ensure the **spark-submit** utility is installed and accessible in Airflow.
5. Go to **Admin > Connections** in Airflow and add the connection details for:
   - **Snowflake**
   - **Spark**

### Step 4: HDFS Setup
1. Place the **car rental raw data** in the appropriate **HDFS location** for processing by the Spark job.

### Step 5: Configuration Files
1. Review and configure the following files:
   - **airflow_dag.py**: Ensure the correct host, port, table names, and credentials are configured.
   - **spark_job.py**: Verify the Spark job configuration (e.g., HDFS location, dimension table names, etc.).
   
### Step 6: Environment Setup
- After the setup is complete, place the **DAG file** in the **DAG directory** of your Airflow account.
- Pass the **execution date** to the DAG, and it will start executing the batch data ingestion pipeline.

## Execution:

- Once the environment is set up and the files are configured correctly, trigger the pipeline from Airflow with the desired **execution date**.
- The pipeline will perform the following:
  1. Execute the SCD2 merge on the customer data in Snowflake.
  2. Submit the Spark job to process the car rental data and load the results into Snowflake.

## Conclusion:
This pipeline automates the ingestion, transformation, and loading of car rental data, ensuring that historical data is properly tracked and that daily rental data is processed and stored in Snowflake for reporting and analysis.