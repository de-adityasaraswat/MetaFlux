import os
import json
import shutil
from pyspark.sql import SparkSession
import pyspark.sql.functions as F # Import F for safe column access
from MetaFlux.src.core.engine import PipelineEngine
from MetaFlux.src.core.interfaces import AbstractReader, AbstractWriter, AbstractAnonymizer

# --- 1. MOCK IMPLEMENTATIONS ---
class MockCSVReader(AbstractReader):
    def read(self, spark, config, context):
        print(f"  [Plugin] Reading CSV: {config.path}")
        df= spark.read.option("header", "true").csv(config.path)
        df.show()
        return df
    def generate_code(self, render_func) -> str: return ""

class MockCSVWriter(AbstractWriter):
    def write(self, spark, config, transformations):
        print(f"  [Plugin] Writing {config.write_view_name} to {config.path}")
        df = spark.table(config.write_view_name)
        df.write.mode(config.mode).option("header","true").csv(config.path)
    def generate_code(self, render_func) -> str: return ""

class MockHashAnonymizer(AbstractAnonymizer):
    def apply(self, df, column_name, params):
        print(f"  [Custom Plugin] Applying Hash to {column_name}")
        # FIXED: The first argument of withColumn must be the string name, not F.col()
        return df.withColumn(column_name, F.sha2(F.col(column_name), 256))
    def generate_code(self, render_func) -> str: return ""

# --- 2. THE TEST SCRIPT ---
def run_test():
    input_rel_path = "test_data_input.csv"
    output_rel_dir = "test_output_folder"
    abs_output_path = os.path.abspath(output_rel_dir)

    # --- STEP A: Setup dummy data ---
    with open(input_rel_path, "w") as f:
        f.write("item,price,customer_email,ssn\n"
                "apple,1.0,john.doe@gmail.com,123-456-7890\n"
                "banana,0.5,jane.smith@yahoo.com,987-654-3210\n")

    # --- STEP B: The Advanced JSON Configuration ---
    user_regex_json_str = r"""
    {
      "security_templates": {
        "base_pii_policy": {
          "customer_email": {
            "action": "regexmask",
            "regex_pattern": "(^.).*(@.*$)",
            "replacement": "$1***$2"
          }
        },
        "extended_privacy_policy": {
          "extends": "base_pii_policy",
          "ssn": {
            "action": "regexmask",
            "regex_pattern": "(\\d{3})-(\\d{3})-(\\d{4})",
            "replacement": "XXX-XXX-$3"
          }
        }
      },
      "reader": {
        "groceries_ds": {
          "type": "csv_batch",
          "path": "test_data_input.csv",
          "view_name": "vw_groceries"
        }
      },
      "transformation": [
        {
          "id": "step_1_enrich",
          "type": "sql",
          "query": "select *, '{{ project_name }}' as project FROM {{ ref('vw_groceries') }}",
          "view_name": "vw_enriched",
          "depends_on": ["vw_groceries"]
        },
        {
          "id": "step_2_apply_template",
          "type": "anonymize",
          "source_view": "vw_enriched",
          "view_name": "vw_secured_by_template",
          "policy_ref": "extended_privacy_policy",
          "depends_on": ["vw_enriched"]
        },
        {
          "id": "step_3_apply_inline",
          "type": "anonymize",
          "source_view": "vw_secured_by_template",
          "view_name": "vw_final_ultimate",
          "inline_policy": {
            "item": {
              "action": "regexmask",
              "regex_pattern": "^.*$",
              "replacement": "REDACTED_ITEM"
            }
          },
          "depends_on": ["vw_secured_by_template"]
        }
      ],
      "writer": {
        "w1": {
          "write_view_name": "vw_final_ultimate",
          "type": "csv_batch",
          "path": "test_output_folder",
          "mode": "overwrite"
        }
      },
      "tokens": [
        {"name": "project_name", "value": "grocery_analytics"}
      ]
    }
    """

    spark = SparkSession.builder.master("local[*]").appName("AdvancedFrameworkTest").getOrCreate()
    sc = spark.sparkContext
    try:
        log_manager = sc._jvm.org.apache.log4j.LogManager
        error_level = sc._int_jvm.org.apache.log4j.Level.ERROR if hasattr(sc, '_int_jvm') else sc._jvm.org.apache.log4j.Level.ERROR
        sc._jvm.org.apache.log4j.LogManager.getLogger("org.apache.hadoop").setLevel(error_level)
        sc._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark").setLevel(error_level)
    except: pass

    engine = PipelineEngine(spark, user_regex_json_str)
    engine.register_reader("csv_batch", MockCSVReader())
    engine.register_writer("csv_batch", MockCSVWriter())
    engine.register_anonymizer("custom_hash", MockHashAnonymizer())

    print("🚀 STARTING ADVANCED PIPELINE TEST...")
    try:
        engine.run()
        print("\n✅ TEST PASSED SUCCESSFULLY")

        print("\n🔍 PERFORMING SECURITY AUDIT ON OUTPUT...")
        result_df = spark.read.option("header", "true").csv(os.path.join(abs_output_path, "part-*.csv"))
        result_df.show()

        # USE F.col() to avoid AttributeError on column access
        # 1. Check Token
        token_check = result_df.filter(F.col("item") == "REDACTED_ITEM").select("project").collect()
        assert len(token_check) > 0, "❌ No rows found!"
        assert token_check[0][0] == "grocery_analytics", f"❌ Token failed! Got {token_check[0][0]}"
        print("  [CHECK] ✅ Token Injection Verified")

        # 2. Check Email (Inherited)
        email_val = result_df.filter(F.col("item") == "REDACTED_ITEM").select("customer_email").collect()[0][0]
        assert email_val.startswith("j***@"), f"❌ Email masking failed! Got {email_val}"
        print("  [CHECK] ✅ Email Masking Verified")

        # 3. Check SSN (Extended)
        ssn_val = result_df.filter(F.col("item") == "REDACTED_ITEM").select("ssn").collect()[0][0]
        assert ssn_val.startswith("XXX-XXX-"), f"❌ SSN masking failed! Got {ssn_val}"
        print("  [CHECK] ✅ SSN Masking Verified")

        # 4. Check Item (Inline)
        item_val = result_df.filter(F.col("item") == "REDACTED_ITEM").select("item").collect()[0][0]
        assert item_val == "REDACTED_ITEM", f"❌ Item redaction failed! Got {item_val}" # FIXED typo here
        print("  [CHECK] ✅ Item Redaction Verified")

        print("\n✨ ALL MULTI-STAGE SECURITY TESTS PASSED ✨")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(input_rel_path): os.remove(input_rel_path)
        if os.path.exists(output_rel_dir): shutil.rmtree(output_rel_dir)
        spark.stop()

if __name__ == "__main__":
    run_test()
