import os
import uuid
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Type, Any, cast

from airflow.providers.pysparkonk8s.resources import CPU, Memory
from airflow.providers.pysparkonk8s.utils import (
    get_namespace,
    get_service_account,
    get_aws_role_arn,
    get_hostname,
    get_ip,
)
from kubernetes.client import models as k8s


class SparkDeployMode(Enum):
    """Nomen est omen"""
    LOCAL = "local"
    CLIENT = "client"
    CONNECT = "connect"


class Sentinel(Enum):
    """Nomen est omen"""
    AUTODETECT = "__AUTODETECT__"


DEFAULT_KUBERNETES_API_URL = "k8s://https://kubernetes.default.svc.cluster.local:443"
DEFAULT_SPARK_CONNECT_SERVER_URL = "sc://spark-connect.spark-connect.svc.cluster.local:15002"
SPARK_DRIVER_AFFINITY_ID_LABEL = "spark-driver-affinity-id"
SPARK_EXECUTOR_AFFINITY_ID_LABEL = "spark-executor-affinity-id"

class AutoTemplateFieldsMeta(ABCMeta):
    """
    Metaclass that adds an automatically generated *template_fields* class attribute to its members.

    *template_fields* are used by Airflow when resolving Jinja templates. Airflow considers the field names present in
    template_fields for templating while rendering the operator.
    """

    def __new__(
            cls: Type[ABCMeta],
            name: str,
            bases: tuple[Type, ...],
            class_dict: dict[str, Any]
    ) -> "AutoTemplateFieldsMeta":
        """
        This method is responsible for creating and returning the new class object.

        :param name: the name of the new class being created
        :param bases: a tuple of base classes for the new class
        :param class_dict: a dictionary of attributes for the new class
        :return: the new class object
        """
        # Get all the attribute names excluding methods, private members and properties from the current class
        template_fields = {
            attr for attr in class_dict if
            not callable(class_dict[attr]) and
            not attr.startswith("_") and
            not isinstance(class_dict[attr], property)
        }
        # Traverse the MRO and collect template fields from parent classes
        for base in bases:
            if hasattr(base, "template_fields"):
                template_fields.update(getattr(base, "template_fields"))
        # Add the combined template fields as a class attribute
        class_dict["template_fields"] = tuple(template_fields)
        result = super().__new__(cls, name, bases, class_dict)
        return cast(AutoTemplateFieldsMeta, result)


class _SparkConf(metaclass=AutoTemplateFieldsMeta):
    """Abstract base class representing a Spark Session configuration management object."""

    @abstractmethod
    def render_spark_conf(self) -> dict[str, str]:
        """
        Renders the Spark Session configuration as a dictionary.

        :return: dictionary holding the Spark Session configuration.
        """
        raise NotImplementedError("This method is not implemented!")


@dataclass(kw_only=True)
class _CommonConf(_SparkConf):
    """
        cores: Specifies the total CPU cores to allow Spark applications to use on the machine. Sets the
            `spark.{driver|executor}.cores` Spark configuration.
        request_cores: Specifies the cpu request for the driver pod. Sets the
            `spark.kubernetes.{driver|executor}.request.cores` Spark configuration.
        memory: Total amount of memory to allow Spark applications to use on the machine.
        memory_overhead: The non-heap memory atop of the assigned memory.
        memory_overhead_factor: Memory fraction to be allocated as additional non-heap memory. This value is ignored if
            memory_overhead is set directly.
        image: Docker image to use.
        container_name: Name of the container to use.
        service_account_name: Name of the Kubernetes service account where the Pods will be created.
        aws_role_arn: ARN of the Amazon IAM role for Service Accounts to be assumed by the Pod.
        extra_java_options: A string of extra JVM options.
        extra_class_path: Extra classpath entries to prepend to the classpath.
        extra_library_path: Set a special library path to use when launching the driver JVM.
    """
    cores: CPU | None = None
    request_cores: CPU | None = CPU.cores(1)
    memory: Memory | None = Memory.gibibytes(1)
    memory_overhead: Memory | None = None
    memory_overhead_factor: float | None = None
    image: str = "sebastiandaberdaku/airflow:2.8.1-python3.10-java17-pyspark3.5.0"
    container_name: str | None = "spark"
    service_account_name: Literal[Sentinel.AUTODETECT] | str | None = "airflow-worker"
    aws_role_arn: Literal[Sentinel.AUTODETECT] | str | None = Sentinel.AUTODETECT
    extra_java_options: str | None = "-XX:+ExitOnOutOfMemoryError"
    extra_class_path: str | None = "/opt/spark/jars/*"
    extra_library_path: str | None = "/opt/hadoop/lib/native"

    @property
    @abstractmethod
    def _component(self) -> Literal["driver", "executor"]:
        """
        Abstract method that defines the Spark role that the current Configuration object is intended for.

        :return: Either the string "driver" or "executor".
        """
        raise NotImplementedError("This method is not implemented!")

    def get_service_account(self) -> str:
        return get_service_account() if self.service_account_name is Sentinel.AUTODETECT else self.service_account_name

    def get_aws_role_arn(self) -> str:
        return get_aws_role_arn() if self.aws_role_arn is Sentinel.AUTODETECT else self.aws_role_arn

    def render_spark_conf(self) -> dict[str, str]:
        spark_conf = {}
        if self.cores is not None:
            spark_conf[f"spark.{self._component}.cores"] = self.cores.to_jvm_spec()
        if self.request_cores is not None:
            spark_conf[f"spark.kubernetes.{self._component}.request.cores"] = self.request_cores.to_k8s_spec()
        if self.memory is not None:
            spark_conf[f"spark.{self._component}.memory"] = self.memory.to_jvm_spec()
        if self.memory_overhead is not None:
            spark_conf[f"spark.{self._component}.memoryOverhead"] = self.memory_overhead.to_jvm_spec()
        if self.memory_overhead_factor is not None:
            spark_conf[f"spark.{self._component}.memoryOverheadFactor"] = str(self.memory_overhead_factor)
        if self.image is not None:
            spark_conf[f"spark.kubernetes.{self._component}.container.image"] = self.image
        if self.container_name is not None:
            spark_conf[f"spark.kubernetes.{self._component}.podTemplateContainerName"] = self.container_name
        if self.service_account_name is not None:
            spark_conf[f"spark.kubernetes.authenticate.{self._component}.serviceAccountName"] = \
                self.get_service_account()
        if self.aws_role_arn is not None:
            spark_conf[f"spark.kubernetes.{self._component}.annotation.eks.amazonaws.com/role-arn"] = \
                self.get_aws_role_arn()
        if self.extra_java_options is not None:
            spark_conf[f"spark.{self._component}.extraJavaOptions"] = self.extra_java_options
        if self.extra_class_path is not None:
            spark_conf[f"spark.{self._component}.extraClassPath"] = self.extra_class_path
        if self.extra_library_path is not None:
            spark_conf[f"spark.{self._component}.extraLibraryPath"] = self.extra_library_path
        return spark_conf


@dataclass(kw_only=True)
class SparkBaseConf(_SparkConf):
    """
    Spark configuration.

    This utility object enables users to customize the Spark configuration.
    Except *deploy_mode*, each configuration can be reset by setting it to None, thereby reverting to the default Spark
    settings for the respective parameter.

    Attributes:
        deploy_mode: Specifies the Spark deploy mode. Possible values are the SparkDeployMode enumeration values.
            Sets the `spark.submit.deployMode` configuration for CLIENT AND CLUSTER deploy modes.
        spark_url: Specifies the `spark.master` or the `spark.remote` configuration value depending on the deployment
            mode. If not specified, the following default values are used:
            "local[*]" for SparkDeployMode.LOCAL;
            "k8s://https://kubernetes.default.svc.cluster.local:443" for SparkDeployMode.CLIENT;
            "sc://spark-connect.spark-connect.svc.cluster.local:15002" for SparkDeployMode.REMOTE.
        kubernetes_namespace: Specifies the namespace that will be used for running the driver and executor pods. If
            AUTODETECT is provided, the current namespace where the Airflow is running will be used. Sets the
            `spark.kubernetes.namespace` Spark configuration.
        image_pull_policy: Container image pull policy used when pulling images within Kubernetes. Valid values are
            Always, Never, and IfNotPresent. Sets the `spark.kubernetes.container.image.pullPolicy` Spark configuration.
        image_pull_secrets: Comma separated list of Kubernetes secrets used to pull images from private image
            registries. Sets the `spark.kubernetes.container.image.pullSecrets` Spark configuration.
        shuffle_file_buffer: Size of the in-memory buffer for each shuffle file output stream. These buffers reduce the
            number of disk seeks and system calls made in creating intermediate shuffle files. Sets the
            `spark.shuffle.file.buffer` Spark configuration.
        jars: A list of Maven coordinates of jars to include on the driver and executor classpaths. The coordinates
            should be groupId:artifactId:version.
    """
    deploy_mode: SparkDeployMode = SparkDeployMode.CLIENT
    spark_url: str | None = None
    kubernetes_namespace: Literal[Sentinel.AUTODETECT] | str | None = Sentinel.AUTODETECT
    kubernetes_ca_cert_file: str | None = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    kubernetes_oauth_token_file: str | None = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    image_pull_policy: Literal["Always", "Never", "IfNotPresent"] | None = "IfNotPresent"
    image_pull_secrets: str | None = None
    shuffle_file_buffer: Memory | None = Memory.mebibytes(1)
    jars: list | None = field(default_factory=list)

    def get_namespace(self) -> str:
        return get_namespace() if self.kubernetes_namespace == Sentinel.AUTODETECT else self.kubernetes_namespace

    def render_spark_conf(self) -> dict[str, str | None]:
        spark_conf = {}
        match self.deploy_mode:
            case SparkDeployMode.LOCAL:
                spark_conf[f"spark.master"] = self.spark_url or "local[*]"
            case SparkDeployMode.CLIENT:
                spark_conf[f"spark.master"] = self.spark_url or DEFAULT_KUBERNETES_API_URL
                spark_conf[f"spark.submit.deployMode"] = self.deploy_mode.value
            case SparkDeployMode.CONNECT:
                spark_conf[f"spark.remote"] = self.spark_url or DEFAULT_SPARK_CONNECT_SERVER_URL

        if self.kubernetes_namespace is not None:
            spark_conf[f"spark.kubernetes.namespace"] = self.get_namespace()
        if self.kubernetes_ca_cert_file is not None:
            spark_conf[f"spark.kubernetes.authenticate.submission.caCertFile"] = self.kubernetes_ca_cert_file
        if self.kubernetes_oauth_token_file is not None:
            spark_conf[f"spark.kubernetes.authenticate.submission.oauthTokenFile"] = self.kubernetes_oauth_token_file
        if self.shuffle_file_buffer is not None:
            spark_conf[f"spark.shuffle.file.buffer"] = self.shuffle_file_buffer.to_jvm_spec()
        if self.image_pull_policy is not None:
            spark_conf[f"spark.kubernetes.container.image.pullPolicy"] = self.image_pull_policy
        if self.image_pull_secrets is not None:
            spark_conf[f"spark.kubernetes.container.image.pullSecrets"] = self.image_pull_secrets
        if self.jars:
            spark_conf["spark.jars.packages"] = ",".join(self.jars)
        return spark_conf


@dataclass(kw_only=True)
class _K8sConf(_CommonConf):
    """
    Represents the Kubernetes-relates configuration options for Spark driver and executor pods.

    Inherits from `_CommonConf` and includes additional Kubernetes-specific configuration options.

    Attributes:
        tolerations: List of Kubernetes tolerations for pod scheduling.
        node_selector: Dictionary representing Kubernetes node selector labels for pod scheduling.
        node_affinity: Kubernetes node affinity rules for pod scheduling.
        pod_affinity: Kubernetes pod affinity rules for pod scheduling.
        pod_anti_affinity: Kubernetes pod anti-affinity rules for pod scheduling.
        containers` List of Kubernetes container specifications for the pod.
        volumes: List of Kubernetes volume specifications for the pod.
        volume_mounts: List of Kubernetes volume mount specifications for the pod.
        environment_variables: List of Kubernetes environment variables for the pod.
        pod_labels: Dictionary representing additional Kubernetes labels for the pod.
    """
    __doc__ += _CommonConf.__doc__

    tolerations: list[k8s.V1Toleration] | None = None
    node_selector: dict[str, str] | None = None
    node_affinity: k8s.V1NodeAffinity | None = None
    pod_affinity: k8s.V1PodAffinity | None = None
    pod_anti_affinity: k8s.V1PodAntiAffinity | None = None
    containers: list[k8s.V1Container] | None = None
    volumes: list[k8s.V1Volume] | None = None
    volume_mounts: list[k8s.V1VolumeMount] | None = None
    environment_variables: list[k8s.V1EnvVar] | None = None
    pod_labels: dict[str, str] | None = None

    @property
    def request_memory(self) -> Memory | None:
        if self.memory is None:
            return None
        request_memory = self.memory
        if self.memory_overhead is not None:
            request_memory += self.memory_overhead
        elif self.memory_overhead_factor is not None:
            request_memory *= self.memory_overhead_factor
        return request_memory

    @abstractmethod
    def _render_environment_variables(self) -> list[k8s.V1EnvVar] | None:
        raise NotImplementedError("This method is not implemented!")

    @abstractmethod
    def _render_pod_labels(self, affinity_id: str) -> dict[str, str]:
        raise NotImplementedError("This method is not implemented!")

    @abstractmethod
    def _render_pod_affinity(self, affinity_id: str) -> k8s.V1PodAffinity | None:
        raise NotImplementedError("This method is not implemented!")

    def _render_containers(self) -> list[k8s.V1Container]:
        if self.containers is not None:
            return self.containers
        requests = None
        limits = None
        if self.request_cores is not None:
            requests = {"cpu": self.request_cores.to_k8s_spec()}
        if self.request_memory is not None:
            requests = {"memory": self.request_memory.to_k8s_spec()} | (requests or {})
            limits = {"memory": self.request_memory.to_k8s_spec()}

        return [k8s.V1Container(
            name=self.container_name,
            image=self.image,
            env=self._render_environment_variables(),
            resources=k8s.V1ResourceRequirements(requests=requests, limits=limits),
            volume_mounts=self.volume_mounts
        )]

    def render_pod_specification(self) -> k8s.V1Pod:
        affinity_id = str(uuid.uuid4())
        return k8s.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s.V1ObjectMeta(labels=self._render_pod_labels(affinity_id=affinity_id)),
            spec=k8s.V1PodSpec(
                dns_config=k8s.V1PodDNSConfig(options=[k8s.V1PodDNSConfigOption(name="ndots", value="2")]),
                containers=self._render_containers(),
                tolerations=self.tolerations,
                node_selector=self.node_selector,
                affinity=k8s.V1Affinity(
                    node_affinity=self.node_affinity,
                    pod_affinity=self._render_pod_affinity(affinity_id=affinity_id),
                    pod_anti_affinity=self.pod_anti_affinity
                ),
                volumes=self.volumes
            )
        )


@dataclass(kw_only=True)
class SparkDriverConf(_K8sConf):
    """
    This utility object enables users to customize the Spark driver-specific configuration.

    Each configuration can be reset by setting it to None, thereby reverting to the default Spark settings for the
    respective parameter.

    Attributes:
        pod_name: Name of the driver pod. By default, the name of the driver pod will be auto-detected.
        host: Hostname or IP address for the driver. By default, the IP of the driver pod will be auto-detected.
    """
    __doc__ += _K8sConf.__doc__
    container_name: Literal["base"] = field(default="base", init=False)
    pod_name: Literal[Sentinel.AUTODETECT] | str | None = Sentinel.AUTODETECT
    host: Literal[Sentinel.AUTODETECT] | str | None = Sentinel.AUTODETECT

    def _render_environment_variables(self) -> list[k8s.V1EnvVar] | None:
        return self.environment_variables if self.environment_variables is not None else [
            k8s.V1EnvVar(name="SPARK_DRIVER_POD_NAME", value_from=k8s.V1EnvVarSource(
                field_ref=k8s.V1ObjectFieldSelector(field_path="metadata.name"))),
            k8s.V1EnvVar(name="SPARK_DRIVER_POD_IP", value_from=k8s.V1EnvVarSource(
                field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP"))),
            k8s.V1EnvVar(name="SPARK_DRIVER_AFFINITY_ID", value_from=k8s.V1EnvVarSource(
                field_ref=k8s.V1ObjectFieldSelector(
                    field_path=f"metadata.labels['{SPARK_DRIVER_AFFINITY_ID_LABEL}']"))),
        ]

    def _render_pod_labels(self, affinity_id: str) -> dict[str, str]:
        return self.pod_labels if self.pod_labels is not None else {SPARK_DRIVER_AFFINITY_ID_LABEL: affinity_id}

    def _render_pod_affinity(self, affinity_id: str) -> k8s.V1PodAffinity | None:
        return self.pod_affinity

    @property
    def _component(self) -> Literal["driver"]:
        return "driver"

    def get_pod_name(self) -> str:
        return get_hostname() if self.pod_name == Sentinel.AUTODETECT else self.pod_name

    def get_host(self) -> str:
        return get_ip() if self.host == Sentinel.AUTODETECT else self.host

    def render_spark_conf(self) -> dict[str, str]:
        spark_conf = super().render_spark_conf()
        if self.pod_name is not None:
            spark_conf[f"spark.kubernetes.driver.pod.name"] = self.get_pod_name()
        if self.host is not None:
            spark_conf[f"spark.driver.host"] = self.get_host()
        return spark_conf


@dataclass(kw_only=True)
class SparkExecutorConf(_K8sConf):
    """
    This utility object enables users to customize the Spark executor-specific configuration.

    Each configuration can be reset by setting it to None, thereby reverting to the default Spark settings for the
    respective parameter.

    Attributes:
        instances: Number of executors to run. Sets the `spark.executor.instances` Spark configuration.
        dynamic_allocation_enabled: Whether to use dynamic resource allocation, which scales the number of executors
            registered with this application up and down based on the workload. Controls the
            `spark.dynamicAllocation.enabled` and `spark.dynamicAllocation.shuffleTracking.enabled` Spark
            configurations.
        dynamic_allocation_min_executors: Lower bound for the number of executors if dynamic allocation is enabled.
            Sets the `spark.dynamicAllocation.minExecutors` Spark configuration.
        dynamic_allocation_max_executors: Upper bound for the number of executors if dynamic allocation is enabled.
            Sets the `spark.dynamicAllocation.maxExecutors` Spark configuration.
    """

    __doc__ += _K8sConf.__doc__
    image: str = "spark:3.5.0-scala2.12-java17-python3-ubuntu"
    instances: int | None = 1
    dynamic_allocation_enabled: bool | None = None
    dynamic_allocation_min_executors: int | None = None
    dynamic_allocation_max_executors: Literal["infinity"] | int | None = None

    def _render_environment_variables(self) -> list[k8s.V1EnvVar] | None:
        return self.environment_variables

    def _render_pod_labels(self, affinity_id: str) -> dict[str, str]:
        return self.pod_labels if self.pod_labels is not None else {SPARK_EXECUTOR_AFFINITY_ID_LABEL: affinity_id}

    def _render_pod_affinity(self, affinity_id: str) -> k8s.V1PodAffinity | None:
        if self.pod_affinity is not None:
            return self.pod_affinity
        driver_affinity_id = os.environ.get("SPARK_DRIVER_AFFINITY_ID", "")
        return k8s.V1PodAffinity(
            preferred_during_scheduling_ignored_during_execution=[
                # try to spawn executors on the same node as the driver pod
                k8s.V1WeightedPodAffinityTerm(weight=100, pod_affinity_term=k8s.V1PodAffinityTerm(
                    label_selector=k8s.V1LabelSelector(match_expressions=[k8s.V1LabelSelectorRequirement(
                        key=SPARK_DRIVER_AFFINITY_ID_LABEL, operator="In", values=[driver_affinity_id])]),
                    topology_key="kubernetes.io/hostname")),
                # otherwise, try to spawn executors on the same node as the other executor pods
                k8s.V1WeightedPodAffinityTerm(weight=75, pod_affinity_term=k8s.V1PodAffinityTerm(
                    label_selector=k8s.V1LabelSelector(match_expressions=[k8s.V1LabelSelectorRequirement(
                        key=SPARK_EXECUTOR_AFFINITY_ID_LABEL, operator="In", values=[affinity_id])]),
                    topology_key="kubernetes.io/hostname")),
                # otherwise, try to spawn executors in the same availability zone as the driver pod
                k8s.V1WeightedPodAffinityTerm(weight=50, pod_affinity_term=k8s.V1PodAffinityTerm(
                    label_selector=k8s.V1LabelSelector(match_expressions=[k8s.V1LabelSelectorRequirement(
                        key=SPARK_DRIVER_AFFINITY_ID_LABEL, operator="In", values=[driver_affinity_id])]),
                    topology_key="topology.kubernetes.io/zone")),
                # otherwise, try to spawn executors in the same availability zone as the other executor pods
                k8s.V1WeightedPodAffinityTerm(weight=25, pod_affinity_term=k8s.V1PodAffinityTerm(
                    label_selector=k8s.V1LabelSelector(match_expressions=[k8s.V1LabelSelectorRequirement(
                        key=SPARK_EXECUTOR_AFFINITY_ID_LABEL, operator="In", values=[affinity_id])]),
                    topology_key="topology.kubernetes.io/zone")),
            ]
        )

    @property
    def _component(self) -> Literal["executor"]:
        return "executor"

    @property
    def pod_template_path(self) -> str:
        """Returns the path where the Pod template file will be rendered."""
        return "/tmp/executor_pod_template.yaml"

    def render_spark_conf(self) -> dict[str, str]:
        spark_conf = super().render_spark_conf()
        if self.instances is not None:
            spark_conf[f"spark.executor.instances"] = str(self.instances)
        if self.dynamic_allocation_enabled is not None:
            spark_conf[f"spark.dynamicAllocation.enabled"] = str(self.dynamic_allocation_enabled).lower()
        if self.dynamic_allocation_min_executors is not None:
            spark_conf[f"spark.dynamicAllocation.minExecutors"] = str(self.dynamic_allocation_min_executors)
        if self.dynamic_allocation_max_executors is not None:
            spark_conf[f"spark.dynamicAllocation.maxExecutors"] = str(self.dynamic_allocation_max_executors)
        spark_conf[f"spark.kubernetes.executor.podTemplateFile"] = self.pod_template_path
        return spark_conf
