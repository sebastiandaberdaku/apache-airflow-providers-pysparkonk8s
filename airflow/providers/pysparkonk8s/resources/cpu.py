from typing import TypeVar, Type, Sequence, Any
import re

# Create a generic variable that can be of type "CPU", or any subclass.
C = TypeVar("C", bound="CPU")


class CPU:
    template_fields: Sequence[str] = ("m_cores",)

    def __init__(self: C, m_cores: int) -> None:
        if type(m_cores) is not int:
            raise TypeError("\"milli_cores\"'s type must be int!")
        if m_cores <= 0:
            raise ValueError("The CPU resource units must be greater than 0!")
        self.m_cores = m_cores

    _k8s_pattern = re.compile(r"^(?P<value>\d+\.?\d*|\d*\.?\d+)(?P<unit>m?)$")

    def __add__(self: C, other: C) -> C:
        if isinstance(other, CPU):
            return CPU(m_cores=self.m_cores + other.m_cores)
        else:
            raise TypeError("Unsupported operand type for \"+\"!")

    def __mul__(self: C, other: C) -> C:
        if isinstance(other, int):
            return CPU(m_cores=self.m_cores * other)
        elif isinstance(other, float):
            return CPU.cores(cores=(self.m_cores / 1000) * other)
        else:
            raise TypeError("Unsupported operand type for \"*\"!")

    def __eq__(self: C, other: C) -> bool:
        if isinstance(other, CPU):
            return self.m_cores == other.m_cores
        else:
            raise TypeError("Unsupported operand type for \"==\"!")

    def __lt__(self: C, other: C) -> bool:
        if isinstance(other, CPU):
            return self.m_cores < other.m_cores
        else:
            raise TypeError("Unsupported operand type for \"<\"!")

    @classmethod
    def cores(cls: Type[C], cores: float) -> C:
        return cls(m_cores=int(cores * 1000))

    @classmethod
    def milli_cores(cls: Type[C], m_cores: int) -> C:
        return cls(m_cores=m_cores)

    @classmethod
    def from_k8s_spec(cls: Type[C], quantity: int | float | str) -> C:
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
        return f"{self.m_cores}m"

    @classmethod
    def from_jvm_spec(cls: Type[C], quantity: int | str) -> C:
        if type(quantity) is int:
            return cls(m_cores=quantity * 1000)
        if type(quantity) is str:
            return cls(m_cores=int(quantity) * 1000)
        else:
            raise TypeError("\"quantity\" type must be str or int!")

    def to_jvm_spec(self: C) -> str:
        return str(int(self.m_cores // 1000))

    def __str__(self) -> str:
        return self.to_k8s_spec()

    def serialize(self) -> int:
        return self.m_cores

    @staticmethod
    def deserialize(data: int, version: int):
        return CPU(m_cores=data)
