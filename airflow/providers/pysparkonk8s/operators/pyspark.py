from contextlib import contextmanager
from typing import Any, Callable, Generator, Mapping, Sequence, TYPE_CHECKING

import yaml
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import create_pod_id
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.providers.pysparkonk8s.config import (
    SparkBaseConf,
    SparkDriverConf,
    SparkExecutorConf,
    SparkDeployMode,
)
from airflow.providers.pysparkonk8s.utils import ensure_spark_is_kwarg, is_test_run

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from airflow.utils.context import Context


class PySparkOnK8sOperator(PythonOperator):
    template_fields: Sequence[str] = (
        *PythonOperator.template_fields,
        "spark_base_conf",
        "spark_driver_conf",
        "spark_executor_conf",
        "spark_extra_conf",
    )
    template_fields_renderers: Mapping[str, str] = {
        **PythonOperator.template_fields_renderers,
        "spark_base_conf": "json",
        "spark_driver_conf": "json",
        "spark_executor_conf": "json",
        "spark_extra_conf": "json"
    }
    FOREST_GREEN = "#228b22"
    ui_color = FOREST_GREEN

    def __init__(
            self,
            *,
            python_callable: Callable,
            spark_base_conf: SparkBaseConf | None = None,
            spark_driver_conf: SparkDriverConf | None = None,
            spark_executor_conf: SparkExecutorConf | None = None,
            spark_extra_conf: dict[str, str] | None = None,
            **kwargs: Any,
    ):
        python_callable = ensure_spark_is_kwarg(python_callable)

        self.spark_base_conf = spark_base_conf or SparkBaseConf()
        self.spark_driver_conf = spark_driver_conf or SparkDriverConf()
        self.spark_executor_conf = spark_executor_conf or SparkExecutorConf()
        self.spark_extra_conf = spark_extra_conf or {}

        self.spark_conf: dict[str, str] = {}

        if self.spark_base_conf.deploy_mode == SparkDeployMode.CLIENT:
            # Override Airflow Worker/Spark Driver Pod specification.
            # When using the KubernetesExecutor, Airflow offers the ability to override system defaults on a per-task
            # basis. To utilize this functionality, create a Kubernetes V1Pod object and fill in your desired overrides.
            # Please note that the scheduler will override the metadata.name and containers[0].args of the V1Pod before
            # launching it.
            # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html#pod-override
            driver_pod_override = {"pod_override": self.spark_driver_conf.render_pod_specification()}
            kwargs["executor_config"] = kwargs.get("executor_config", {}) | driver_pod_override

        super().__init__(
            python_callable=python_callable,
            **kwargs
        )

    def _render_spark_conf(self) -> None:
        """
        Initializes the Spark configuration for the Spark Session. This method is called once before the Spark session
        is initialized. It merges the Spark configuration from the Spark config objects, and from the `spark_extra_conf`
        attribute, which can be used to override specific configurations.
        :return: None
        """
        self.spark_conf["spark.app.name"] = create_pod_id(self.dag_id, self.task_id, unique=False)
        self.spark_conf.update(self.spark_base_conf.render_spark_conf())
        self.spark_conf.update(self.spark_driver_conf.render_spark_conf())
        self.spark_conf.update(self.spark_executor_conf.render_spark_conf())

        for conf_key, conf_val in self.spark_extra_conf.items():
            if conf_val is None:
                self.spark_conf.pop(conf_key, None)
            else:
                self.spark_conf[conf_key] = conf_val

    def _get_spark_conf_as_string(self) -> str:
        """
        Returns a printable version of the Spark configuration, as a list of key=value pairs, one per line.
        """
        assert self.spark_conf is not None
        return "\n".join(f"{k}={v}" for k, v in self.spark_conf.items())

    def _save_spark_executor_pod_template_to_disk(self) -> None:
        """
        This method saves the Spark executor pod template to disk, so it can be picked up by Spark when starting the
        cluster in Client and Cluster modes.
        :return: None
        """
        pod = self.spark_executor_conf.render_pod_specification()
        sanitized_pod = PodGenerator.serialize_pod(pod)
        with open(self.spark_executor_conf.pod_template_path, "w") as f:
            yaml.dump(sanitized_pod, f, default_flow_style=False)

    @contextmanager
    def _get_spark_session(self) -> Generator["SparkSession", None, None]:
        """
        Context manager for acquiring and managing a SparkSession instance within a controlled scope.
        Note:
            This method is intended to be used as a context manager using the 'with' statement.
            It handles the initialization, configuration, and cleanup of a SparkSession.
        Yields:
            SparkSession: An initialized SparkSession instance.
        Raises:
            Any exceptions raised during SparkSession initialization or usage.
        """
        from pyspark.sql import SparkSession
        spark = None
        try:
            self.log.info("Initializing Spark configuration...")
            self._render_spark_conf()
            if self.spark_base_conf.deploy_mode == SparkDeployMode.CLIENT:
                self._save_spark_executor_pod_template_to_disk()
            self.log.info(f"Spark configuration:\n{self._get_spark_conf_as_string()}")
            self.log.info("Starting Spark session...")
            spark = SparkSession.builder.config(map=self.spark_conf).getOrCreate()
            self.log.info("Spark session started.")
            yield spark
        finally:
            if spark is not None and not is_test_run():
                self.log.info("Stopping Spark session...")
                spark.stop()
                self.log.info("Spark session stopped.")

    def execute(self, context: "Context") -> Any:
        """
        This is the operator method that executes the python callable. We make sure to provide the initialized Spark
        Session before actually executing.
        :param context: The same dictionary used as when rendering jinja templates.
        :return: The return value of the python callable.
        """
        with self._get_spark_session() as spark:
            # inject the Spark Session as kwarg into the python_callable
            if "spark" in self.python_callable.__signature__.parameters:
                self.op_kwargs["spark"] = spark
            return super().execute(context)
