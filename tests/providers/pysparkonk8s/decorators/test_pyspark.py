from typing import TYPE_CHECKING
from unittest import mock

from airflow.decorators import dag, task
from airflow.providers.pysparkonk8s.config import SparkBaseConf, SparkDeployMode
from airflow.providers.pysparkonk8s.decorators import pyspark_on_k8s_task
from kubernetes.client import models as k8s
from pendulum import datetime

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@mock.patch("pyspark.sql.SparkSession")
def test_pyspark_k8s_decorator(spark_session_mock):
    @pyspark_on_k8s_task(spark_base_conf=SparkBaseConf(deploy_mode=SparkDeployMode.CLIENT))
    def pyspark_function(spark: "SparkSession"):
        assert spark is not None
        d = [{'name': 'Alice', 'age': 1}]
        spark.createDataFrame(d).collect()

    @dag(
        dag_id="my_dag",
        start_date=datetime(2024, 1, 1),
        schedule=None,
    )
    def pytest_dag():
        pyspark_function()

    pytest_dag_inst = pytest_dag()

    executor_config = pytest_dag_inst.get_task("pyspark_function").executor_config
    assert isinstance(executor_config["pod_override"], k8s.V1Pod)

    pytest_dag_inst.test()
    spark_session_mock.builder.config.assert_called_once()
    spark_session_mock.builder.config().getOrCreate.assert_called_once()


@mock.patch("pyspark.sql.SparkSession")
def test_pyspark_k8s_taskflow_decorator(spark_session_mock):
    @task.pyspark_on_k8s(spark_base_conf=SparkBaseConf(deploy_mode=SparkDeployMode.CLIENT))
    def pyspark_function(spark: "SparkSession"):
        assert spark is not None
        d = [{'name': 'Alice', 'age': 1}]
        spark.createDataFrame(d).collect()

    @dag(
        dag_id="my_dag",
        start_date=datetime(2024, 1, 1),
        schedule=None,
    )
    def pytest_dag():
        pyspark_function()

    pytest_dag_inst = pytest_dag()

    executor_config = pytest_dag_inst.get_task("pyspark_function").executor_config
    assert isinstance(executor_config["pod_override"], k8s.V1Pod)

    pytest_dag_inst.test()
    spark_session_mock.builder.config.assert_called_once()
    spark_session_mock.builder.config().getOrCreate.assert_called_once()
