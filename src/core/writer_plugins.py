from pyspark.sql import SparkSession
from .interfaces import AbstractWriter


class CSVWriter(AbstractWriter):
    def write(self, spark: SparkSession, config, transformations: dict) -> None:
        print(f"  [Plugin] Writing CSV to: {config.path}")
        df = spark.table(config.write_view_name)
        writer = df.write.mode(config.mode).csv(config.path)
        if config.partition_by:
            writer = writer.partitionBy(*config.partition_by)
        writer.save()


class JSONWriter(AbstractWriter):
    def write(self, spark: SparkSession, config, transformations: dict) -> None:
        print(f"  [Plugin] Writing JSON to: {config.path}")
        df = spark.table(config.write_view_name)
        df.write.mode(config.mode).json(config.path)


class ParquetWriter(AbstractWriter):
    def write(self, spark: SparkSession, config, transformations: dict) -> None:
        print(f"  [Plugin] Writing Parquet to: {config.path}")
        df = spark.table(config.write_view_name)
        writer = df.write.mode(config.mode).parquet(config.path)
        if config.partition_by:
            writer = writer.partitionBy(*config.partition_by)
        writer.save()


class DeltaWriter(AbstractWriter):
    def write(self, spark: SparkSession, config, transformations: dict) -> None:
        print(f"  [Plugin] Writing Delta to: {config.path}")
        df = spark.table(config.write_view_name)
        writer = df.write.format("delta").mode(config.mode)
        if config.partition_by:
            writer = writer.partitionBy(*config.partition_by)
        writer.save(config.path)
