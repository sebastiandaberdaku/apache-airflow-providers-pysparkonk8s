import logging
from random import randint
from time import sleep
from typing import TYPE_CHECKING

from airflow.decorators import dag, task
from airflow.decorators.base import Task, FReturn
from airflow.decorators.task_group import FParams
from airflow.providers.pysparkonk8s.config import SparkExecutorConf
from pendulum import datetime, duration

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)

@task
def choose_executors() -> int:
    """Returns a random integer among 1, 2, and 3."""
    return randint(1, 3)


def parametrized_spark_task(n_executors: int) -> Task[FParams, FReturn]:
    """Returns a PySparkOnK8s task with variable number of executors depending on an in put parameter."""
    sec = SparkExecutorConf(instances=n_executors)

    @task.pyspark_on_k8s(spark_executor_conf=sec)
    def custom_task(spark: "SparkSession") -> None:
        """
        Simple Pyspark task that creates a DataFrame with some sample data and prints it.
        The task will sleep for 120 seconds to give users time to view the spawned pods on Kubernetes.

        :param spark: SparkSession object provided by the @task.pyspark_on_k8s decorator.
        :return: None
        """
        from pyspark.sql import types as t
        data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1),
        ]

        schema = t.StructType([
            t.StructField("firstname", t.StringType(), True),
            t.StructField("middlename", t.StringType(), True),
            t.StructField("lastname", t.StringType(), True),
            t.StructField("id", t.StringType(), True),
            t.StructField("gender", t.StringType(), True),
            t.StructField("salary", t.IntegerType(), True),
        ])

        df = spark.createDataFrame(data=data, schema=schema)
        df.printSchema()
        sleep(120)
        df.show()

    return custom_task


@dag(
    dag_id="example_dag_3",
    start_date=datetime(2024, 1, 1),
    doc_md="""
        Example DAG showcasing the dynamic parameter passing in tasks created with the `@task.pyspark_on_k8s` decorator. 
        
        An initial task returns a random number of executors. This number will be used to parametrize the second task.
        
        This logic can be similarly extended to other parameters such as memory, CPUs, etc.
    """,
    schedule=None,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
    }
)
def example_dag_3():
    n_executors = choose_executors()
    spark_task = parametrized_spark_task(n_executors=n_executors)
    n_executors >> spark_task()


example_dag_3()
