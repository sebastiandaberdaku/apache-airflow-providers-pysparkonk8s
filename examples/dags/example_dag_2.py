import logging
from typing import TYPE_CHECKING

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.pysparkonk8s.config import SparkBaseConf, SparkExecutorConf
from airflow.providers.pysparkonk8s.operators import PySparkOnK8sOperator
from pendulum import datetime, duration

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)

@task()
def create_bucket(s3_bucket: str, region: str) -> str:
    """
    Creates an S3 bucket if it does not already exist.

    :param s3_bucket: The name of the S3 bucket to be created.
    :param region: The AWS region in which the S3 bucket should be created.
    :return: The name of the S3 bucket.
    """
    s3_hook = S3Hook(aws_conn_id="aws")
    logger.info(f"Checking if S3 bucket {s3_bucket} exists...")
    if not s3_hook.check_for_bucket(bucket_name=s3_bucket):
        logger.info(f"S3 bucket {s3_bucket} does not exist. Creating...")
        s3_hook.create_bucket(bucket_name=s3_bucket, region_name=region)
        logger.info(f"Created S3 bucket {s3_bucket} in region {region}.")
    else:
        logger.info(f"S3 bucket {s3_bucket} already exists.")
    return s3_bucket

@task()
def extract(url: str, s3_bucket: str) -> str:
    """
    Downloads a file from the specified URL and uploads it to an S3 bucket.

    :param url: The URL of the file to be downloaded.
    :param s3_bucket: S3 bucket where the file will be uploaded.
    :return: The S3A URI of the uploaded file.
    """
    s3_hook = S3Hook(aws_conn_id="aws")
    filename = url.split("/")[-1]
    s3_key = f"extract/{filename}"
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        s3_hook.load_file_obj(file_obj=response.raw, key=s3_key, bucket_name=s3_bucket, replace=True)
    return f"s3a://{s3_bucket}/{s3_key}"


# The following configuration is required to run Airflow on AWS/LocalStack.
S3_ENDPOINT = "http://localstack.localstack.svc.cluster.local:4566"
sbc = SparkBaseConf(jars=["com.amazonaws:aws-java-sdk-bundle:1.12.262", "org.apache.hadoop:hadoop-aws:3.3.4"])
sec = SparkExecutorConf(instances=2)
extra_conf = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,  # Required since we want to use LocalStack instead of the actual S3.
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",  # No SSL for LocalStack.
    "spark.hadoop.fs.s3a.access.key": "foo",  # We still need to provide some fake credentials.
    "spark.hadoop.fs.s3a.secret.key": "bar",  # Normally Spark would use the IAM role for Service Accounts.
}

def load(s3a_uri: str, s3_bucket: str, spark: "SparkSession") -> str:
    """
    Loads the given CSV file on S3 to parquet format.

    :param s3a_uri: The source CSV files to load.
    :param s3_bucket: S3 bucket where the file will be uploaded.
    :param spark: SparkSession object provided by the @task.pyspark_on_k8s decorator.
    :return: S3A destination URI of the resulting parquet files.
    """
    df = spark.read.csv(s3a_uri, header=True, inferSchema=True, ignoreLeadingWhiteSpace=True)
    df.printSchema()
    df.show()

    filename = s3a_uri.split("/")[-1]  # first extract the filename from the S3 key
    folder_name = filename.lower().removesuffix(".csv")  # remove the suffix

    s3a_dst_uri = f"s3a://{s3_bucket}/load/{folder_name}/"
    df.write.mode("overwrite").parquet(s3a_dst_uri)
    return s3a_dst_uri


def transform(s3a_uri: str, s3_bucket: str, spark: "SparkSession") -> str:
    """
    Computes the average rating for each year.

    :param s3a_uri: The source parquet files to load.
    :param s3_bucket: S3 bucket where the file will be uploaded.
    :param spark: SparkSession object provided by the @task.pyspark_on_k8s decorator.
    :return: S3A destination URI of the resulting parquet files.
    """
    df = spark.read.parquet(s3a_uri)
    agg_df = df.groupBy("Year").avg("Score").sort("Year")
    agg_df.printSchema()
    agg_df.show()

    folder_name = s3a_uri.lower().removesuffix("/").split("/")[-1]

    s3a_dst_uri = f"s3a://{s3_bucket}/transform/{folder_name}/"
    agg_df.write.mode("overwrite").parquet(s3a_dst_uri)
    return s3a_dst_uri


@dag(
    dag_id="example_dag_2",
    start_date=datetime(2024, 1, 1),
    doc_md="""
        Example DAG showing how to use the `PySparkOnK8sOperator`. 
        
        A very simple Extract, Load, Transform (ELT) pipeline is used to extract a CSV file (Rotten Tomato ratings of 
        movies with Robert De Niro) from a public server and save it on an S3 bucket, then load it into parquet format, 
        and finally transform it by computing the average rating by year.
        
        Data source: [here](https://people.sc.fsu.edu/~jburkardt/data/data.html)
    """,
    schedule=None,
    params={
        "url": "https://people.sc.fsu.edu/~jburkardt/data/csv/deniro.csv",
        "s3_bucket": "another-test-bucket",
        "region": "eu-central-1",
    },
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
    }
)
def example_dag_2():
    s3_bucket = create_bucket(s3_bucket="{{ params.s3_bucket }}", region="{{ params.region }}")
    csv_s3a_uri = extract(url="{{ params.url }}", s3_bucket="{{ params.s3_bucket }}")
    s3_bucket >> csv_s3a_uri
    load_s3a_uri = PySparkOnK8sOperator(
        task_id="load",
        python_callable=load,
        spark_base_conf=sbc,
        spark_executor_conf=sec,
        spark_extra_conf=extra_conf,
        op_kwargs={
            "s3a_uri": csv_s3a_uri,
            "s3_bucket": "{{ params.s3_bucket }}"
        }
    )
    PySparkOnK8sOperator(
        task_id="transform",
        python_callable=transform,
        spark_base_conf=sbc,
        spark_executor_conf=sec,
        spark_extra_conf=extra_conf,
        op_kwargs={
            "s3a_uri": load_s3a_uri.output,
            "s3_bucket": "{{ params.s3_bucket }}"
        }
    )


example_dag_2()
