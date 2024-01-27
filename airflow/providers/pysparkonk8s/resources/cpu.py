from typing import TypeVar, Type, Sequence, Any
import re

# Create a generic variable that can be of type "CPU", or any subclass.
C = TypeVar("C", bound="CPU")


class CPU:
    """Represents a CPU resource and provides conversion methods between Kubernetes and JVM specifications."""

    def __init__(self: C, m_cores: int) -> None:
        """
        Initializes a CPU object.

        :param m_cores: The milli cores value for the CPU.

        :raises TypeError: If `m_cores` is not of type `int`.
        :raises ValueError: If `m_cores` is less than or equal to 0.
        """
        if type(m_cores) is not int:
            raise TypeError("\"milli_cores\"'s type must be int!")
        if m_cores <= 0:
            raise ValueError("The CPU resource units must be greater than 0!")
        self.m_cores = m_cores

    _k8s_pattern = re.compile(r"^(?P<value>\d+\.?\d*|\d*\.?\d+)(?P<unit>m?)$")

    def __add__(self: C, other: C) -> C:
        """
        Adds two CPU objects.

        :param other: The CPU object to be added.
        :return: A new CPU object representing the sum of the two CPUs.
        :raises TypeError: If `other` is not of type `CPU`.
        """
        if isinstance(other, CPU):
            return CPU(m_cores=self.m_cores + other.m_cores)
        else:
            raise TypeError("Unsupported operand type for \"+\"!")

    def __mul__(self: C, other: C) -> C:
        """
        Multiplies a CPU object by either an integer or a float.

        :param other: The multiplier.
        :return: A new CPU object representing the result of the multiplication.
        :raises TypeError: If `other` is not of type `int` or `float`.
        """
        if isinstance(other, int):
            return CPU(m_cores=self.m_cores * other)
        elif isinstance(other, float):
            return CPU.cores(cores=(self.m_cores / 1000) * other)
        else:
            raise TypeError("Unsupported operand type for \"*\"!")

    def __eq__(self: C, other: C) -> bool:
        """
        Checks if two CPU objects are equal.

        :param other: The CPU object to compare.
        :return: True if the CPUs are equal, False otherwise.
        :raises TypeError: If `other` is not of type `CPU`.
        """
        if isinstance(other, CPU):
            return self.m_cores == other.m_cores
        else:
            raise TypeError("Unsupported operand type for \"==\"!")

    def __lt__(self: C, other: C) -> bool:
        """
        Checks if one CPU object is less than another.

        :param other: The CPU object to compare.
        :return: True if the first CPU is less than the second, False otherwise.
        :raises TypeError: If `other` is not of type `CPU`.
        """
        if isinstance(other, CPU):
            return self.m_cores < other.m_cores
        else:
            raise TypeError("Unsupported operand type for \"<\"!")

    @classmethod
    def cores(cls: Type[C], cores: float) -> C:
        """
        Creates a CPU object from a specified number of cores.

        :param cores: The number of cores.
        :return: A new CPU object.
        """
        return cls(m_cores=int(cores * 1000))

    @classmethod
    def milli_cores(cls: Type[C], m_cores: int) -> C:
        """
        Creates a CPU object from a specified number of milli cores.

        :param m_cores: The number of milli cores.
        :return: A new CPU object.
        """
        return cls(m_cores=m_cores)

    @classmethod
    def from_k8s_spec(cls: Type[C], quantity: int | float | str) -> C:
        """
        Creates a CPU object from a Kubernetes specification.

        :param quantity: The Kubernetes specification.
        :return: A new CPU object.
        :raises ValueError: If the provided quantity is not a valid Kubernetes memory spec.
        :raises TypeError: If `quantity` is not of type `int`, `float`, or `str`.
        """
        if type(quantity) in [int, float]:
            return cls(m_cores=int(quantity * 1000))
        if type(quantity) is str:
            match = cls._k8s_pattern.match(quantity)
            if match:
                value = float(match.group("value"))
                unit = match.group("unit")
                m_cores: float = value if unit == "m" else value * 1000
                return cls(m_cores=int(m_cores))
            else:
                raise ValueError(f"The provided quantity {quantity} is not a valid Kubernetes memory spec!")
        else:
            raise TypeError("\"quantity\"'s type must be str!")

    def to_k8s_spec(self: C) -> str:
        """
        Converts the CPU object to a Kubernetes specification string.

        :return: The Kubernetes specification string.
        """
        return f"{self.m_cores}m"

    @classmethod
    def from_jvm_spec(cls: Type[C], quantity: int | str) -> C:
        """
        Creates a CPU object from a JVM specification.

        :param quantity: The JVM specification.
        :return: A new CPU object.
        :raises TypeError: If `quantity` is not of type `int` or `str`.
        """
        if type(quantity) is int:
            return cls(m_cores=quantity * 1000)
        if type(quantity) is str:
            return cls(m_cores=int(quantity) * 1000)
        else:
            raise TypeError("\"quantity\" type must be str or int!")

    def to_jvm_spec(self: C) -> str:
        """
        Converts the CPU object to a JVM specification string.

        :return: The JVM specification string.
        """
        return str(int(self.m_cores // 1000))

    def __str__(self) -> str:
        """
        Returns a string representation of the CPU object.

        :return: The string representation.
        """
        return self.to_k8s_spec()

    def serialize(self) -> int:
        """
        Method used by Airflow to serialize a CPU object.
        :return: The serialized CPU object (i.e. its milli cores).
        """
        return self.m_cores

    @staticmethod
    def deserialize(data: int, version: int):
        """
        Method used by Airflow to deserialize a CPU object.

        :param data: The serialized object representation (i.e. its milli cores).
        :param version: The object version information (not used in this implementation).
        :return: The deserialized CPU object.
        """
        return CPU(m_cores=data)
