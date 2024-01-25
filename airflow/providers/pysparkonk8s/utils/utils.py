import inspect
import os
import socket
import sys
from typing import Callable

import jwt


def ensure_spark_is_kwarg(func: Callable) -> Callable:
    """
    Ensures that the "spark" argument, if present, is a keyword argument with default value set to None.
    This ensures that any callable with a "spark" argument can be decorated with @task.spark_on_k8s or used by
    the PySparkOnK8s operator.
    The function also sets the __signature__ attribute of the callable.

    :param func: python callable
    :return: same callable with __signature__ attribute set and "spark" keyword argument (if present).
    """
    signature = inspect.signature(func)
    parameters = [
        param.replace(default=None) if param.name == "spark" else param for param in signature.parameters.values()
    ]
    func.__signature__ = signature.replace(parameters=parameters)
    return func


def get_hostname() -> str:
    """
    Returns the hostname of the current machine.

    :return: The current machine's hostname.
    """
    return socket.gethostname()


def get_namespace() -> str:
    """
    Returns the kubernetes namespace of the current Pod. The namespace is read from the service account namespace token
    file mounted by Kubernetes. The default path can be overriden by setting the `K8S_NAMESPACE_TOKEN_PATH` environment
    variable.

    :return: The current kubernetes namespace.
    """
    default_service_account_namespace_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    namespace_path = os.environ.get("K8S_NAMESPACE_TOKEN_PATH", default_service_account_namespace_path)
    with open(namespace_path, "r") as k8s_namespace_file:
        return k8s_namespace_file.read().strip()


def get_service_account() -> str:
    """
    Returns the kubernetes service account of the current Pod. The service account name is read from the service account
    jwt token file mounted by Kubernetes. The default path can be overriden by setting the
    K8S_SERVICE_ACCOUNT_TOKEN_PATH` environment variable.

    :return: The current kubernetes service account.
    """
    default_sa_token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    sa_token_path = os.environ.get("K8S_SERVICE_ACCOUNT_TOKEN_PATH", default_sa_token_path)
    with open(sa_token_path, "r") as k8s_service_account_token:
        encoded_token = k8s_service_account_token.read()
        payload = jwt.decode(encoded_token, options={"verify_signature": False})
        return payload.get("kubernetes.io", "default").get("serviceaccount", "default").get("name", "default")


def get_ip() -> str:
    """
    Returns the IP address of the current Pod.

    :return: The IP address of the current Pod.
    """
    # Create a UDP socket (AF_INET indicates IPv4, SOCK_DGRAM indicates UDP).
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Try to connect to any IP, even non-existent (240.0.0.0 is a reserved multicast address).
        s.connect(("240.0.0.0", 0))
        # Get the local IP address from the connected socket.
        ip = s.getsockname()[0]
        return ip
    finally:
        # Close the socket to release resources.
        s.close()


def get_aws_role_arn() -> str:
    """
    Returns the AWS Role ARN of the current Pod.

    :return: The AWS Role ARN of the current Pod.
    """
    return os.environ.get("AWS_ROLE_ARN", "default")


def is_test_run() -> bool:
    """
    Checks if the current run is a test.

    :return: True if the code is running in a test, False otherwise.
    """
    return (os.environ.get("AIRFLOW_TEST") is not None or
            os.environ.get("PYTEST_CURRENT_TEST") is not None or
            os.environ.get("PYTEST_RUNNING_TEST") is not None or
            "unittest" in sys.modules or
            "pytest" in sys.modules
            )
