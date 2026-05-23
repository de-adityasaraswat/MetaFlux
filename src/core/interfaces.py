from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import Any


class AbstractReader(ABC):
    @abstractmethod
    def read(self, spark: SparkSession, config: Any, context: dict) -> DataFrame:
        """Runtime execution logic."""
        pass

    @abstractmethod
    def generate_code(self, render_func) -> str:
        """Code generation logic for the compiler."""
        pass

class AbstractWriter(ABC):
    @abstractmethod
    def write(self, spark: SparkSession, config: Any, transformations: dict) -> None: pass

    @abstractmethod
    def generate_code(self, render_func) -> str:
        """Code generation logic for the compiler."""
        pass

class AbstractStep(ABC):
    @abstractmethod
    def execute(self, spark: SparkSession, config: Any, context: dict) -> DataFrame: pass
