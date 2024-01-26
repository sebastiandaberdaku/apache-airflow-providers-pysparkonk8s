import logging
from typing import TYPE_CHECKING

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.pysparkonk8s.config import SparkBaseConf, SparkExecutorConf
from pendulum import datetime, duration

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)

@task()
def create_bucket(s3_bucket: str, region: str) -> None:
    """
    Creates an S3 bucket if it does not already exist.

    :param s3_bucket: The name of the S3 bucket to be created.
    :param region: The AWS region in which the S3 bucket should be created.
    :return: None
    """
    s3_hook = S3Hook(aws_conn_id="aws")
    logger.info(f"Checking if S3 bucket {s3_bucket} exists...")
    if not s3_hook.check_for_bucket(bucket_name=s3_bucket):
        logger.info(f"S3 bucket {s3_bucket} does not exist. Creating...")
        s3_hook.create_bucket(bucket_name=s3_bucket, region_name=region)
        logger.info(f"Created S3 bucket {s3_bucket} in region {region}.")
    else:
        logger.info(f"S3 bucket {s3_bucket} already exists.")

@task()
def extract(url: str, s3_bucket: str) -> str:
    """
    Downloads a file from the specified URL and uploads it to an S3 bucket.

    :param url: The URL of the file to be downloaded.
    :param s3_bucket: The name of the S3 bucket where the file will be uploaded.
    :return: The S3 key of the uploaded file.
    """
    s3_hook = S3Hook(aws_conn_id="aws")
    filename = url.split("/")[-1]
    s3_key = f"extracted/{filename}"
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        s3_hook.load_file_obj(file_obj=response.raw, key=s3_key, bucket_name=s3_bucket, replace=True)
    return s3_key


S3_ENDPOINT = "http://localstack.localstack.svc.cluster.local:4566"
sbc = SparkBaseConf(jars=["com.amazonaws:aws-java-sdk-bundle:1.12.262", "org.apache.hadoop:hadoop-aws:3.3.4"])
sec = SparkExecutorConf(instances=2)
extra_conf = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.access.key": "foo",
    "spark.hadoop.fs.s3a.secret.key": "bar",
}

@task.pyspark_on_k8s(spark_base_conf=sbc, spark_executor_conf=sec, spark_extra_conf=extra_conf)
def load(s3_bucket: str, s3_key: str, spark: "SparkSession") -> str:
    """
    Loads the given CSV file on S3 to parquet format.
    :param s3_bucket: The name of the S3 bucket where the file will be uploaded.
    :param s3_key: The S3 key of the CSV file to load.
    :param spark: SparkSession object provided by the @task.pyspark_on_k8s decorator
    :return: S3A destination URI of the resulting parquet files.
    """
    s3a_uri = f"s3a://{s3_bucket}/{s3_key}"
    df = spark.read.csv(s3a_uri, header=True, inferSchema=True)
    print(df.head(5))

    s3a_dst_uri = f"s3a://{s3_bucket}/parquet/"
    df.write.mode("overwrite").parquet(s3a_dst_uri)
    return s3a_dst_uri


@dag(
    dag_id="example_dag_1",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    params={
        "url": "https://people.sc.fsu.edu/~jburkardt/data/csv/deniro.csv",
        "s3_bucket": "test-bucket",
        "region": "eu-central-1",
    },
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
    }
)
def example_dag_1():
    csv_s3_key = extract(url="{{ params.url }}", s3_bucket="{{ params.s3_bucket }}")

    create_bucket(s3_bucket="{{ params.s3_bucket }}", region="{{ params.region }}") >> csv_s3_key

    load(s3_bucket="{{ params.s3_bucket }}", s3_key=csv_s3_key)


example_dag_1()
