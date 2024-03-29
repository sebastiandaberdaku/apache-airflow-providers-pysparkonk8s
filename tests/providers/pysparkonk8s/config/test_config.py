import pytest
from airflow.providers.pysparkonk8s.config import SparkBaseConf, SparkDriverConf, SparkExecutorConf
from kubernetes.client import models as k8s


@pytest.fixture
def spark_base_conf():
    return SparkBaseConf()


@pytest.fixture
def spark_driver_conf():
    return SparkDriverConf()


@pytest.fixture
def spark_executor_conf():
    return SparkExecutorConf()


def test_base_conf_template_fields():
    assert set(SparkBaseConf.template_fields) == {
        "deploy_mode", "spark_url", "kubernetes_namespace", "kubernetes_ca_cert_file", "kubernetes_oauth_token_file",
        "image_pull_policy", "image_pull_secrets", "shuffle_file_buffer", "jars"
    }


def test_driver_conf_template_fields():
    assert set(SparkDriverConf.template_fields) == {
        "pod_affinity", "image", "volume_mounts", "node_selector", "memory_overhead", "environment_variables",
        "pod_anti_affinity", "pod_labels", "extra_class_path", "memory", "cores", "tolerations", "extra_java_options",
        "service_account_name", "volumes", "aws_role_arn", "containers", "node_affinity", "pod_name", "request_cores",
        "extra_library_path", "host", "container_name", "memory_overhead_factor"
    }

def test_executor_conf_template_fields():
    print(SparkExecutorConf.template_fields)
    assert set(SparkExecutorConf.template_fields) == {
        "node_affinity", "container_name", "memory_overhead", "request_cores", "service_account_name",
        "pod_anti_affinity", "memory_overhead_factor", "pod_labels", "memory", "node_selector", "pod_affinity",
        "aws_role_arn", "containers", "volumes", "extra_library_path", "dynamic_allocation_max_executors",
        "volume_mounts", "instances", "extra_class_path", "tolerations", "cores", "environment_variables",
        "extra_java_options", "dynamic_allocation_enabled", "image", "dynamic_allocation_min_executors"
    }


@pytest.mark.parametrize("config_key", [
    "spark.master",
    "spark.submit.deployMode",
    "spark.kubernetes.namespace",
    "spark.kubernetes.authenticate.submission.caCertFile",
    "spark.kubernetes.authenticate.submission.oauthTokenFile",
    "spark.shuffle.file.buffer",
    "spark.kubernetes.container.image.pullPolicy",
])
def test_spark_base_conf(spark_base_conf, config_key):
    conf = spark_base_conf.render_spark_conf()
    assert config_key in conf.keys()


@pytest.mark.parametrize("config_key", [
    "spark.kubernetes.driver.request.cores",
    "spark.driver.memory",
    "spark.kubernetes.driver.container.image",
    "spark.kubernetes.driver.podTemplateContainerName",
    "spark.kubernetes.authenticate.driver.serviceAccountName",
    "spark.kubernetes.driver.annotation.eks.amazonaws.com/role-arn",
    "spark.driver.extraJavaOptions",
    "spark.driver.extraClassPath",
    "spark.driver.extraLibraryPath",
    "spark.kubernetes.driver.pod.name",
    "spark.driver.host",
])
def test_spark_driver_conf(spark_driver_conf, config_key):
    conf = spark_driver_conf.render_spark_conf()
    assert config_key in conf.keys()


@pytest.mark.parametrize("config_key", [
    "spark.kubernetes.executor.request.cores",
    "spark.executor.memory",
    "spark.kubernetes.executor.container.image",
    "spark.kubernetes.executor.podTemplateContainerName",
    "spark.kubernetes.authenticate.executor.serviceAccountName",
    "spark.kubernetes.executor.annotation.eks.amazonaws.com/role-arn",
    "spark.executor.extraJavaOptions",
    "spark.executor.extraClassPath",
    "spark.executor.extraLibraryPath",
    "spark.executor.instances",
    "spark.kubernetes.executor.podTemplateFile",
])
def test_spark_executor_conf(spark_executor_conf, config_key):
    conf = spark_executor_conf.render_spark_conf()
    assert config_key in conf.keys()


def test_driver_pod_spec(spark_driver_conf):
    pod = spark_driver_conf.render_pod_specification()
    assert isinstance(pod, k8s.V1Pod)
    assert "spark-driver-affinity-id" in pod.metadata.labels
    environment_variables = {e.name for e in pod.spec.containers[0].env}
    assert "SPARK_DRIVER_AFFINITY_ID" in environment_variables


def test_executor_pod_spec(spark_executor_conf, spark_driver_affinity_id):
    pod = spark_executor_conf.render_pod_specification()
    assert isinstance(pod, k8s.V1Pod)
    pod_affinities = pod.spec.affinity.pod_affinity.preferred_during_scheduling_ignored_during_execution

    spark_driver_affinity_ids = [
        a.pod_affinity_term.label_selector.match_expressions[0].values[0] for a in pod_affinities
        if a.pod_affinity_term.label_selector.match_expressions[0].key == "spark-driver-affinity-id"
    ]
    for affinity_id in spark_driver_affinity_ids:
        assert affinity_id == spark_driver_affinity_id
