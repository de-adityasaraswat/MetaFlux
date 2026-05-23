from pyspark.sql import SparkSession, DataFrame
from .interfaces import AbstractReader

class CSVReader(AbstractReader):
    def read(self, spark: SparkSession, config, context: dict) -> DataFrame:
        print(f"  [Plugin] Reading CSV: {config.path}")
        return spark.read.option("header", str(config.header).lower()) \
            .option("inferSchema", str(config.infer_schema).lower()) \
            .csv(config.path)

    def generate_code(self, render_func) -> str:
        path = render_func(self.config.path)
        return (f"# CSV Reader\n"
                f"df_{self.config.view_name} = spark.read.option('header', True)\n"
                f"    .option('inferSchema', True)\n"
                f"    .csv('{path}')\n"
                f"spark.createOrReplaceTempView('{self.config.view_name}')")

class JSONReader(AbstractReader):
    def read(self, spark: SparkSession, config, context: dict) -> DataFrame:
        print(f"  [Plugin] Reading JSON: {config.path}")
        return spark.read.option("multiline", "true").json(config.path)

class ParquetReader(AbstractReader):
    def read(self, spark: SparkSession, config, context: dict) -> DataFrame:
        print(f"  [Plugin] Reading Parquet: {config.path}")
        return spark.read.parquet(config.path)

class DeltaReader(AbstractReader):
    def read(	self, spark: SparkSession, config, context: dict) -> DataFrame:
        print(f"  [Plugin] Reading Delta from: {config.path}")
        reader = spark.read.format("delta")
        # Handle Time Travel / Versioning if provided in options
        if hasattr(config, 'options') and 'versionAsOf' in config.options:
            reader = reader.option("versionAsOf", config.options['version_as_of'])
        return reader.load(config.path)

    def generate_code(self, render_func) -> str:
        path = render_func(self.config.path)
        code = [f"# Delta Reader\ndf_{self.config.view_name} = spark.read.format('delta').load('{path}')"]
        # If versioning is needed, the plugin handles it internally
        return "\n".join(code)
