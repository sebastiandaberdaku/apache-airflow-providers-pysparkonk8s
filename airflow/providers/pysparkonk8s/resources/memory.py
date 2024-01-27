import re
from abc import ABCMeta
from enum import IntEnum, EnumMeta
from typing import TypeVar, Type

# Create a generic variable that can be of type "Memory", or any subclass.
M = TypeVar("M", bound="Memory")
# Create a generic variable that can be of type "Unit", or any subclass.
U = TypeVar("U", bound="Unit")


class ABCEnumMeta(EnumMeta, ABCMeta):
    """Metaclass for creating enumeration classes with both enum and abstract class features."""
    pass


class Unit(IntEnum, metaclass=ABCEnumMeta):
    """Abstract base class for defining memory units as Enum."""

    @classmethod
    def get_unit_pattern(cls: Type[U]) -> re.Pattern[str]:
        """
        Returns a compiled regular expression pattern for matching memory units.
        """
        unit_names = [unit.name for unit in cls]
        unit_pattern = "|".join(unit_names)
        return re.compile(rf"^(?P<value>\d+\.?\d*|\d*\.?\d+)(?P<unit>{unit_pattern})?$")


class K8sUnit(Unit):
    """
    Enumeration representing Kubernetes memory units.

    The values in this enumeration correspond to the power-of-two equivalents
    and power-of-ten equivalents of memory units used in Kubernetes.

    :cvar int Ei: Exbibytes (2^60 bytes)
    :cvar int Pi: Pebibytes (2^50 bytes)
    :cvar int Ti: Tebibytes (2^40 bytes)
    :cvar int Gi: Gibibytes (2^30 bytes)
    :cvar int Mi: Mebibytes (2^20 bytes)
    :cvar int Ki: Kibibytes (2^10 bytes)
    :cvar int E: Exabytes (10^18 bytes)
    :cvar int P: Petabytes (10^15 bytes)
    :cvar int T: Terabytes (10^12 bytes)
    :cvar int G: Gigabytes (10^9 bytes)
    :cvar int M: Megabytes (10^6 bytes)
    :cvar int k: Kilobytes (10^3 bytes)
    """
    Ei = 2 ** 60
    Pi = 2 ** 50
    Ti = 2 ** 40
    Gi = 2 ** 30
    Mi = 2 ** 20
    Ki = 2 ** 10
    E = 10 ** 18
    P = 10 ** 15
    T = 10 ** 12
    G = 10 ** 9
    M = 10 ** 6
    k = 10 ** 3


class JvmUnit(Unit):
    """
    Enumeration representing JVM memory units.

    The values in this enumeration correspond to the power-of-two equivalents
    of memory units used in Java Virtual Machines (JVM).

    :cvar int t: Tebibytes (2^40 bytes)
    :cvar int g: Gibibytes (2^30 bytes)
    :cvar int m: Mebibytes (2^20 bytes)
    :cvar int k: Kibibytes (2^10 bytes)
    """
    t = 2 ** 40
    g = 2 ** 30
    m = 2 ** 20
    k = 2 ** 10


class Memory:
    """Represents a memory resource and provides conversion methods between Kubernetes and JVM specifications."""

    def __init__(self, value: int | float, unit: Unit | None = None) -> None:
        """
        Initializes a Memory object with the given value in memory Units. The memory size is internally converted in its
        integer size in bytes.

        :param float value: The size of the memory in unit bytes.
        :param Unit unit: (Optional) The memory unit of measure. If no unit is provided, the memory value is intended in
        bytes.

        :raises TypeError: If the provided value is not a number or if the provided unit is not of type Unit.
        :raises ValueError: If the size in bytes is less than or equal to 0.
        """
        if not type(value) in [int, float]:
            raise TypeError("\"value\" must be a float or an int!")
        if unit is not None and not isinstance(unit, Unit):
            raise TypeError("\"unit\" must be of type Unit!")

        multiplier = unit.value if unit is not None else 1
        size_in_bytes = int(value * multiplier)
        if size_in_bytes <= 0:
            raise ValueError("The memory size in bytes must be greater than 0!")

        self.size_in_bytes: int = size_in_bytes

    def __add__(self: M, other: M) -> M:
        """
        Adds two Memory objects together.

        :param Memory other: The Memory object to be added.
        :return: A new Memory object representing the sum of the two memories.
        :raises TypeError: If the provided operand is not a Memory object.
        """
        if isinstance(other, Memory):
            return Memory(value=self.size_in_bytes + other.size_in_bytes, unit=None)
        else:
            raise TypeError("Unsupported operand type for \"+\"!")

    def __mul__(self: M, other: int | float) -> M:
        """
        Multiplies the Memory object by a scalar.

        :param float|int other: The scalar value to multiply by.
        :return: A new Memory object representing the result of the multiplication.
        :raises TypeError: If the provided operand is not a float or int.
        """
        if isinstance(other, (int, float)):
            return Memory(value=self.size_in_bytes * other)
        else:
            raise TypeError("Unsupported operand type for \"*\"!")

    def __eq__(self: M, other: M) -> bool:
        """
        Compares two Memory objects for equality.

        :param Memory other: The Memory object to compare with.
        :return: True if the memories are equal, False otherwise.
        :raises TypeError: If the provided operand is not a Memory object.
        """
        if isinstance(other, Memory):
            return self.size_in_bytes == other.size_in_bytes
        else:
            raise TypeError("Unsupported operand type for \"==\"!")

    def __lt__(self: M, other: M) -> bool:
        """
        Compares two Memory objects.

        :param Memory other: The Memory object to compare with.
        :return: True if the current memory is less than the other, False otherwise.
        :raises TypeError: If the provided operand is not a Memory object.
        """
        if isinstance(other, Memory):
            return self.size_in_bytes < other.size_in_bytes
        else:
            raise TypeError("Unsupported operand type for \"<\"!")

    @classmethod
    def gibibytes(cls: Type[M], value: float) -> M:
        """
        Creates a Memory object with the specified value in gibibytes.

        :param float value: The value in gibibytes.
        :return: A new Memory object representing the specified value in gibibytes.
        """
        return cls(value=value, unit=K8sUnit.Gi)

    @classmethod
    def gigabytes(cls: Type[M], value: float) -> M:
        """
        Creates a Memory object with the specified value in gigabytes.

        :param float value: The value in gigabytes.
        :return: A new Memory object representing the specified value in gigabytes.
        """
        return cls(value=value, unit=K8sUnit.G)

    @classmethod
    def mebibytes(cls: Type[M], value: float) -> M:
        """
        Creates a Memory object with the specified value in mebibytes.

        :param float value: The value in mebibytes.
        :return: A new Memory object representing the specified value in mebibytes.
        """
        return cls(value=value, unit=K8sUnit.Mi)

    @classmethod
    def megabytes(cls: Type[M], value: float) -> M:
        """
        Creates a Memory object with the specified value in megabytes.

        :param float value: The value in megabytes.
        :return: A new Memory object representing the specified value in megabytes.
        """
        return cls(value=value, unit=K8sUnit.M)

    @classmethod
    def from_k8s_spec(cls: Type[M], quantity: str) -> M:
        """
        Creates a Memory object from a Kubernetes memory specification.

        :param str quantity: The Kubernetes memory specification.
        :return: A Memory object representing the specified Kubernetes memory.
        :raises TypeError: If the provided quantity is not a string.
        :raises ValueError: If the provided quantity is not a valid Kubernetes memory specification.
        """
        if type(quantity) is not str:
            raise TypeError("\"quantity\" must be a string!")

        match = K8sUnit.get_unit_pattern().match(quantity)
        if match:
            value = float(match.group("value"))
            unit = match.group("unit")
            return cls(value=value, unit=K8sUnit[unit] if unit else None)
        else:
            raise ValueError(f"The provided quantity {quantity} is not a valid Kubernetes memory spec!")

    def to_k8s_spec(self, unit: K8sUnit | None = K8sUnit.Mi) -> str:
        """
        Converts the Memory object to a Kubernetes memory specification.
        The resulting value is truncated to the nearest integer unit.

        :param K8sUnit | None unit: The unit to use for the conversion. Defaults to K8sUnit.Mi.
        :return: The Kubernetes memory specification.
        :raises TypeError: If the provided unit is not a Kubernetes unit.
        """
        if unit is None:
            return str(self.size_in_bytes)
        if type(unit) is not K8sUnit:
            raise TypeError("\"unit\" must be a Kubernetes unit!")
        value: int = self.size_in_bytes // unit.value
        return f"{value}{unit.name}"

    @classmethod
    def from_jvm_spec(cls: Type[M], quantity: str) -> M:
        """
        Creates a Memory object from a JVM memory specification.

        :param str quantity: The JVM memory specification.
        :return: A Memory object representing the specified JVM memory.
        :raises TypeError: If the provided quantity is not a string.
        :raises ValueError: If the provided quantity is not a valid JVM memory specification.
        """
        if type(quantity) is not str:
            raise TypeError("\"quantity\" must be a string!")

        match = JvmUnit.get_unit_pattern().match(quantity)
        if match:
            value = int(match.group("value"))
            unit = match.group("unit")
            return cls(value=value, unit=JvmUnit[unit] if unit else None)
        else:
            raise ValueError(f"The provided quantity {quantity} is not a valid JVM memory spec!")

    def to_jvm_spec(self, unit: JvmUnit = JvmUnit.m) -> str:
        """
        Converts the Memory object to a JVM memory specification.
        The resulting value is truncated to the nearest integer unit.

        :param JvmUnit unit: The unit to use for the conversion.
        :return: The JVM memory specification.
        :raises ValueError: If the provided unit is None or not a JVM unit.
        """
        if unit is None:
            raise ValueError("\"unit\" cannot be None!")
        if type(unit) is not JvmUnit:
            raise ValueError("\"unit\" must be a JVM unit!")
        value: int = self.size_in_bytes // unit.value
        return f"{value}{unit.name}"

    def __str__(self) -> str:
        """
        Returns a string representation of the Memory object in Kubernetes specification format.

        :return: The string representation of the Memory object.
        """
        return self.to_k8s_spec(unit=None)

    def serialize(self) -> int:
        """
        Method used by Airflow to serialize a Memory object.
        :return: The serialized Memory object (i.e. its size in bytes).
        """
        return self.size_in_bytes

    @staticmethod
    def deserialize(data: int, version: int):
        """
        Method used by Airflow to deserialize a Memory object.

        :param data: The serialized object representation (i.e. its size in bytes).
        :param version: The object version information (not used in this implementation).
        :return: The deserialized Memory object.
        """
        return Memory(value=data, unit=None)
