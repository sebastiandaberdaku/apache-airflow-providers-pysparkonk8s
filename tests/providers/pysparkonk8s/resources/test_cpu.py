import pytest

from airflow.providers.pysparkonk8s.resources import CPU


def test_create_cpu_with_valid_cores():
    cpu = CPU(m_cores=2000)
    assert cpu.m_cores == 2000


def test_create_cpu_with_invalid_cores_type():
    with pytest.raises(TypeError, match="\"milli_cores\"'s type must be int!"):
        CPU(m_cores="invalid")


def test_create_cpu_with_negative_cores():
    with pytest.raises(ValueError, match="The CPU resource units must be greater than 0!"):
        CPU(m_cores=-1000)


def test_add_two_cpus():
    cpu1 = CPU(m_cores=1000)
    cpu2 = CPU(m_cores=500)
    result = cpu1 + cpu2
    assert result.m_cores == 1500


def test_multiply_cpu_by_scalar():
    cpu = CPU(m_cores=1000)
    result = cpu * 1.5
    assert result.m_cores == 1500


def test_multiply_cpu_by_non_scalar():
    cpu = CPU(m_cores=1000)
    with pytest.raises(TypeError, match=r"Unsupported operand type for \"\*\"!"):
        cpu * CPU(m_cores=500)


def test_compare_equal_cpus():
    cpu1 = CPU(m_cores=1000)
    cpu2 = CPU(m_cores=1000)
    assert cpu1 == cpu2


def test_compare_unequal_cpus():
    cpu1 = CPU(m_cores=1000)
    cpu2 = CPU(m_cores=1500)
    assert cpu1 < cpu2


def test_creating_cpu_from_cores():
    cpu = CPU.cores(2.5)
    assert cpu.m_cores == 2500


def test_creating_cpu_from_milli_cores():
    cpu = CPU.milli_cores(1500)
    assert cpu.m_cores == 1500


def test_creating_cpu_from_k8s_spec():
    cpu = CPU.from_k8s_spec("1.5")
    assert cpu.m_cores == 1500


def test_creating_cpu_from_invalid_k8s_spec():
    with pytest.raises(ValueError, match="The provided quantity invalid is not a valid Kubernetes memory spec!"):
        CPU.from_k8s_spec("invalid")


def test_creating_cpu_from_jvm_spec():
    cpu = CPU.from_jvm_spec("2")
    assert cpu.m_cores == 2000


def test_creating_cpu_from_invalid_jvm_spec():
    with pytest.raises(TypeError, match="\"quantity\" type must be str or int!"):
        CPU.from_jvm_spec(2.5)


def test_to_k8s_spec():
    cpu = CPU(m_cores=1500)
    assert cpu.to_k8s_spec() == "1500m"


def test_to_jvm_spec():
    cpu = CPU(m_cores=2500)
    assert cpu.to_jvm_spec() == "2"
