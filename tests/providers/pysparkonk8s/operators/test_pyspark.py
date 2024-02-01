from unittest.mock import ANY

from airflow.providers.pysparkonk8s.config import SparkDeployMode, SparkBaseConf
from airflow.providers.pysparkonk8s.operators import PySparkOnK8sOperator
from kubernetes.client import models as k8s


def test_pyspark_on_k8s_operator_init():
    operator = PySparkOnK8sOperator(
        python_callable=lambda spark: spark.createDataFrame([("Alice", 1)]).collect(),
        spark_base_conf=SparkBaseConf(deploy_mode=SparkDeployMode.CLIENT),
        task_id="my_test_pyspark_task",
    )

    assert isinstance(operator.executor_config["pod_override"], k8s.V1Pod)


def test_pyspark_on_k8s_operator_render_spark_conf():
    operator = PySparkOnK8sOperator(
        python_callable=lambda spark: spark.createDataFrame([("Alice", 1)]).collect(),
        spark_base_conf=SparkBaseConf(deploy_mode=SparkDeployMode.CLIENT),
        task_id="my_test_pyspark_task",
    )

    operator._render_spark_conf()

    expected_spark_conf = {
        "spark.app.name": "adhoc-airflow-my-test-pyspark-task",
        "spark.master": "k8s://https://kubernetes.default.svc.cluster.local:443",
        "spark.submit.deployMode": "client",
        "spark.kubernetes.namespace": "airflow-test-ns",
        "spark.kubernetes.authenticate.submission.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
        "spark.kubernetes.authenticate.submission.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
        "spark.shuffle.file.buffer": "1m",
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
        "spark.kubernetes.driver.request.cores": "1000m",
        "spark.driver.memory": "1024m",
        "spark.kubernetes.driver.container.image": "sebastiandaberdaku/airflow:2.8.1-python3.10-java17-pyspark3.5.0",
        "spark.kubernetes.driver.podTemplateContainerName": "base",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-worker",
        "spark.kubernetes.driver.annotation.eks.amazonaws.com/role-arn": "default",
        "spark.driver.extraJavaOptions": "-XX:+ExitOnOutOfMemoryError",
        "spark.driver.extraClassPath": "/opt/spark/jars/*",
        "spark.driver.extraLibraryPath": "/opt/hadoop/lib/native",
        "spark.kubernetes.driver.pod.name": "sebastian-ThinkPad-X1-Carbon-Gen-10",
        "spark.driver.host": ANY,
        "spark.kubernetes.executor.request.cores": "1000m",
        "spark.executor.memory": "1024m",
        "spark.kubernetes.executor.container.image": "spark:3.5.0-scala2.12-java17-python3-ubuntu",
        "spark.kubernetes.executor.podTemplateContainerName": "spark",
        "spark.kubernetes.authenticate.executor.serviceAccountName": "airflow-worker",
        "spark.kubernetes.executor.annotation.eks.amazonaws.com/role-arn": "default",
        "spark.executor.extraJavaOptions": "-XX:+ExitOnOutOfMemoryError",
        "spark.executor.extraClassPath": "/opt/spark/jars/*",
        "spark.executor.extraLibraryPath": "/opt/hadoop/lib/native",
        "spark.executor.instances": "1",
        "spark.kubernetes.executor.podTemplateFile": "/tmp/executor_pod_template.yaml"
    }
    assert operator.spark_conf == expected_spark_conf


def test_pyspark_on_k8s_operator_conf_override():
    operator = PySparkOnK8sOperator(
        python_callable=lambda spark: spark.createDataFrame([("Alice", 1)]).collect(),
        spark_base_conf=SparkBaseConf(deploy_mode=SparkDeployMode.CLIENT),
        spark_extra_conf={
            "spark.kubernetes.namespace": "airflow",  # This config will be overridden.
            "spark.kubernetes.executor.podTemplateFile": None,  # This config will be removed.
        },
        task_id="my_test_pyspark_task",
    )

    operator._render_spark_conf()

    expected_spark_conf = {
        "spark.app.name": "adhoc-airflow-my-test-pyspark-task",
        "spark.master": "k8s://https://kubernetes.default.svc.cluster.local:443",
        "spark.submit.deployMode": "client",
        "spark.kubernetes.namespace": "airflow",
        "spark.kubernetes.authenticate.submission.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
        "spark.kubernetes.authenticate.submission.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
        "spark.shuffle.file.buffer": "1m",
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
        "spark.kubernetes.driver.request.cores": "1000m",
        "spark.driver.memory": "1024m",
        "spark.kubernetes.driver.container.image": "sebastiandaberdaku/airflow:2.8.1-python3.10-java17-pyspark3.5.0",
        "spark.kubernetes.driver.podTemplateContainerName": "base",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-worker",
        "spark.kubernetes.driver.annotation.eks.amazonaws.com/role-arn": "default",
        "spark.driver.extraJavaOptions": "-XX:+ExitOnOutOfMemoryError",
        "spark.driver.extraClassPath": "/opt/spark/jars/*",
        "spark.driver.extraLibraryPath": "/opt/hadoop/lib/native",
        "spark.kubernetes.driver.pod.name": "sebastian-ThinkPad-X1-Carbon-Gen-10",
        "spark.driver.host": ANY,
        "spark.kubernetes.executor.request.cores": "1000m",
        "spark.executor.memory": "1024m",
        "spark.kubernetes.executor.container.image": "spark:3.5.0-scala2.12-java17-python3-ubuntu",
        "spark.kubernetes.executor.podTemplateContainerName": "spark",
        "spark.kubernetes.authenticate.executor.serviceAccountName": "airflow-worker",
        "spark.kubernetes.executor.annotation.eks.amazonaws.com/role-arn": "default",
        "spark.executor.extraJavaOptions": "-XX:+ExitOnOutOfMemoryError",
        "spark.executor.extraClassPath": "/opt/spark/jars/*",
        "spark.executor.extraLibraryPath": "/opt/hadoop/lib/native",
        "spark.executor.instances": "1",
    }
    assert operator.spark_conf == expected_spark_conf
