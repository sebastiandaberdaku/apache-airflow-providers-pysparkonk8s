import pytest
from contextlib import nullcontext as does_not_raise

from airflow.providers.pysparkonk8s.resources.memory import Memory, K8sUnit, JvmUnit


@pytest.mark.parametrize("size_in_bytes, expected_error", [
    (0, pytest.raises(ValueError)),
    (0.5, pytest.raises(ValueError)),
    (-1, pytest.raises(ValueError)),
    (1.5, does_not_raise()),
    ("invalid", pytest.raises(TypeError)),
])
def test_memory_init_errors(size_in_bytes, expected_error):
    with expected_error:
        Memory(size_in_bytes)


@pytest.mark.parametrize("quantity, expected_error", [
    ("100Mi", does_not_raise()),
    ("1Gi", does_not_raise()),
    ("512", does_not_raise()),
    (123, pytest.raises(TypeError)),
    ("invalid", pytest.raises(ValueError)),
    ([123], pytest.raises(TypeError)),
])
def test_memory_from_k8s_spec_errors(quantity, expected_error):
    with expected_error:
        Memory.from_k8s_spec(quantity)


@pytest.mark.parametrize("quantity, expected_result", [
    ("100Mi", 104857600),
    ("1Gi", 1073741824),
    ("512", 512),
])
def test_memory_from_k8s_spec(quantity, expected_result):
    memory = Memory.from_k8s_spec(quantity)
    assert memory.size_in_bytes == expected_result


@pytest.mark.parametrize("unit, expected_result", [
    (K8sUnit.Mi, "0Mi"),
    (K8sUnit.Gi, "0Gi"),
    (None, "512"),
])
def test_memory_to_k8s_spec(unit, expected_result):
    memory = Memory(512)
    assert memory.to_k8s_spec(unit) == expected_result


@pytest.mark.parametrize("quantity, expected_error", [
    ("100m", does_not_raise()),
    ("2g", does_not_raise()),
    ("1t", does_not_raise()),
    (123, pytest.raises(TypeError)),
    ("invalid", pytest.raises(ValueError)),
])
def test_memory_from_jvm_spec_errors(quantity, expected_error):
    with expected_error:
        Memory.from_jvm_spec(quantity)


@pytest.mark.parametrize("quantity, expected_result", [
    ("100m", 104857600),
    ("2g", 2147483648),
    ("1t", 1099511627776),
])
def test_memory_from_jvm_spec(quantity, expected_result):
    memory = Memory.from_jvm_spec(quantity)
    assert memory.size_in_bytes == expected_result


@pytest.mark.parametrize("unit, expected_result", [
    (JvmUnit.g, "2g"),
    (JvmUnit.m, "2048m"),
    (JvmUnit.t, "0t"),
])
def test_memory_to_jvm_spec(unit, expected_result):
    memory = Memory(2147483648)  # 2 GiB
    assert memory.to_jvm_spec(unit) == expected_result


@pytest.fixture
def valid_k8s_spec():
    return "1Gi"


@pytest.fixture
def valid_jvm_spec():
    return "2g"


@pytest.fixture
def invalid_spec():
    return "invalid_spec"


def test_memory_object_initialization():
    # Test initialization with valid values
    memory = Memory(value=512, unit=K8sUnit.Mi)
    assert memory.size_in_bytes == 512 * K8sUnit.Mi

    # Test initialization without specifying a unit (default to bytes)
    memory_no_unit = Memory(value=1024)
    assert memory_no_unit.size_in_bytes == 1024

    # Test initialization with invalid value
    with pytest.raises(ValueError):
        Memory(value=-1, unit=K8sUnit.Mi)

    # Test initialization with invalid unit
    with pytest.raises(TypeError):
        Memory(value=1024, unit="invalid_unit")


def test_k8s_spec_conversion():
    # Test converting from Kubernetes spec to Memory object
    memory = Memory.from_k8s_spec("1Gi")
    assert memory.size_in_bytes == 1 * K8sUnit.Gi

    # Test converting to Kubernetes spec
    k8s_spec = memory.to_k8s_spec(unit=K8sUnit.Mi)
    assert k8s_spec == "1024Mi"

    # Test invalid Kubernetes spec
    with pytest.raises(ValueError):
        Memory.from_k8s_spec("invalid_spec")


def test_jvm_spec_conversion():
    # Test converting from JVM spec to Memory object
    memory = Memory.from_jvm_spec("2g")
    assert memory.size_in_bytes == 2 * JvmUnit.g

    # Test converting to JVM spec
    jvm_spec = memory.to_jvm_spec(unit=JvmUnit.m)
    assert jvm_spec == "2048m"

    # Test invalid JVM spec
    with pytest.raises(ValueError):
        Memory.from_jvm_spec("invalid_spec")


def test_type_annotations():
    # Test type annotations for enum members
    assert isinstance(K8sUnit.Mi, int)
    assert isinstance(JvmUnit.m, int)

    # Test type checking in Memory constructor
    with pytest.raises(TypeError):
        Memory(value="invalid_value")


def test_enum_member_values():
    # Test consistency in enum member values
    assert K8sUnit.Mi == 2 ** 20
    assert JvmUnit.m == 2 ** 20
