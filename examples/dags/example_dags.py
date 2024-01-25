from time import sleep
from typing import TYPE_CHECKING

from airflow.decorators import dag, task
from pendulum import datetime

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@task.pyspark_on_k8s
def extract(spark: "SparkSession") -> float:
    from pyspark import SparkFiles
    from pyspark.sql import functions as f

    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/deniro.csv"
    spark.sparkContext.addFile(url)
    df = spark.read.csv("file://" + SparkFiles.get("deniro.csv"), header=True, inferSchema=True)
    df.show()

    sleep(120)

    mean_score, = df.select(f.mean("Score")).first()
    return mean_score


@dag(
    dag_id="example_dag_1",
    start_date=datetime(2024, 1, 1),
    schedule=None,
)
def example_dag_1():
    mean_score = extract()


example_dag_1()
