from enum import Enum


class Scenario(Enum):
    """An enumeration of supported benchmarking scenarios."""

    TPCH = "tpch"

    def __str__(self) -> str:
        return self.value
