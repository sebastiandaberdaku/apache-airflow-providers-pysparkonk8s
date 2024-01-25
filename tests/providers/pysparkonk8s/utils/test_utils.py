import ipaddress
from contextlib import nullcontext as does_not_raise

from airflow.providers.pysparkonk8s.utils import is_test_run, get_ip, get_namespace, get_service_account


def test_get_ip():
    ip = get_ip()
    assert ip != "127.0.0.1"

    with does_not_raise():
        ipv4 = ipaddress.IPv4Address(ip)
        assert ipv4.is_private


def test_is_test_run():
    assert is_test_run()


def test_get_namespace():
    assert get_namespace() == "airflow-test-ns"


def test_get_service_account():
    assert get_service_account() == "airflow-worker"
