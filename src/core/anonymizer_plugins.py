from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .interfaces import AbstractAnonymizer


class RegexMaskAnonymizer(AbstractAnonymizer):
    """
    A highly flexible anonymizer that applies a user-defined regex pattern.
    It looks for 'regex_pattern' and 'replacement' in the JSON configuration.
    """

    def apply(self, df: DataFrame, column_name: str, params: dict) -> DataFrame:
        # 1. Extract pattern and replacement from the JSON 'params'
        # We provide defaults to prevent the pipeline from crashing if a user forgets a key
        regex_pattern = params.get("regex_pattern")
        replacement = params.get("replacement", "***")  # Default replacement is stars

        if not regex_pattern:
            raise ValueError(
                f"Error in column '{column_name}': 'regex_pattern' must be provided "
                f"in the security policy for RegexMask."
            )
        # ENTERPRISE FEATURE:
        # Automatically handle the heavy lifting of regex string cleaning
        # This allows users to write cleaner JSON.
        #clean_pattern = regex_pattern.replace('\\', '\\\\')
        # 2. Apply the transformation using Spark's native regexp_replace
        # This is high-performance and distributed
        return df.withColumn(
            column_name,
            F.regexp_replace(F.col(column_name), regex_pattern, replacement)
        )


class RedactFull(AbstractAnonymizer):
    def apply(self, df: DataFrame, column_name: str, params: dict) -> DataFrame:
        return df.withColumn(column_name, F.lit("****"))


class MaskEmail(AbstractAnonymizer):
    def apply(self, df: DataFrame, column_name: str, params: dict) -> DataFrame:
        # Logic: user@domain.com -> u***@domain.com
        return df.withColumn(column_name,
                             F.regexp_replace(column_name, r"^(.).*(@.*)$", r"$1***$2"))


class MaskPartial(AbstractAnonymizer):
    def apply(self, df: DataFrame, column_name: str, params: dict) -> DataFrame:
        keep = params.get("keep_last", 4)
        return df.withintColumn(column_name,
                                F.concat(F.lit("****"), F.substring(column_name, -keep, keep)))


class EncryptAES(AbstractAnonymizer):
    def apply(self, df: DataFrame, column_name: str, params: dict) -> DataFrame:
        # In production, fetch 'key' from Azure Key Vault or AWS KMS
        secret_key = "super-secret-enterprise-key"
        return df.withColumn(column_name, F.sha2(F.col(column_name), 256))  # Example using SHA256 for demo
