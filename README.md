GCP-datalake-pyspark-airflow-pipeline

![gcppipeline](https://github.com/avmistry3/GCP-datalake-pyspark-airflow-pipeline/assets/51489015/5a269ca7-b3fe-4ecc-b22b-a7a8ea4475a3)


How to run a simple PySpark job on a Dataproc cluster using Airflow.

  Build an ETL (Extract, Transform, Load) pipeline. We'll extract datasets from a data lake (Google Cloud Storage), process the data using PySpark on a Dataproc cluster, and then load the transformed data back into GCS as dimensional tables in parquet format.
The dataset we'll use contains song data and log data. We'll transform this dataset into four dimensional tables and one fact table as part of our ETL process.

Automate Pyspark job and running it with Dataproc Cluster using Airflow

create an Airflow DAG with the following steps:
1.	Create a Cluster: We'll set up a Dataproc cluster with the required settings and machine types.
2.	Execute the PySpark Job: We'll run the PySpark job, which can involve one step or multiple steps to process our data.
3.	Delete the Cluster: Once the job is done, we'll clean up by deleting the Dataproc cluster.

