import os
import json
import shutil
from pyspark.sql import SparkSession
from MetaFlux.src.core.engine import PipelineEngine
from MetaFlux.src.core.interfaces import AbstractReader, AbstractWriter


# 1. Mock Implementments for testing (remains same)
class MockCSVReader(AbstractReader):
    def read(self, spark, config, context):
        print(f"  [Plugin] Reading CSV: {config.path}")
        if not os.path.exists(config.path):
            return spark.createDataFrame([("error", 0)], ["item_name", "price"])
        return spark.read.option("header", "true").csv(config.path)


class MockCSVWriter(AbstractWriter):
    def write(self, spark, config, transformations):
        print(f"  [Plugin] Writing {config.write_view_name} to {config.path}")
        df = spark.table(config.write_view_name)
        df.write.mode(config.mode).csv(config.path)


# 2. The Test Script
def run_test():
    # --- PATH DISCOVERY LOGIC ---

    input_rel_path = "test_data_input.csv"
    output_rel_dir = "test_output_folder"

    abs_input_path = os.path.abspath(input_rel_path)
    abs_output_path = os.path.abspath(output_rel_dir)

    print("\n" + "=" * 60)
    print("📍 FILE PATH LOCATIONS (Find these in Finder)")
    print(f"📂 Input File Path:  {abs_input_path}")
    print(f"📂 Output Folder:    {abs_output_path}")
    print("=" * 60 + "\n")

    # --- STEP A: Setup dummy data ---
    with open(input_rel_path, "w") as f:
        f.write("item,price\napple,1.0\nbanana,0.5\n")

    # --- STEP B: The JSON Configuration ---
    # FIXED: Removed 'ref' from tokens (it is now internal to the engine).
    # ADDED: A business token 'project_name' to test user-defined variable injection.
    user_json_str = """
    {
      "reader": {
        "groceries_ds": {
          "type": "csv_batch",
          "path": "test_data_input.csv",
          "view_name": "vw_groceries"
        }
      },
      "transformation": {
        "add_metadata": {
          "type": "sql",
          "query": "select *, '{{ project_name }}' as project FROM {{ ref('vw_groceries') }}",
          "view_name": "vw_final",
          "depends_on": ["vw_groceries"]
        }
      },
      "writer": {
        "w1": {
          "write_view_name": "vw_final",
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

    # --- STEP C: Initialize Spark and Engine ---
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("FrameworkTest") \
        .getOrCreate()
    # --- 1. SETUP HADOOP LOG LEVEL SUPPRESSION ---
    # This prevents the scary Java/Hadoop stack traces from cluttering your terminal
    sc = SparkSession.builder.getOrCreate()
    sc._jvm.org.apache.log4j.LogManager.getLogger("org.apache.hadoop").setLevel(sc._jvm.org.apache.log4j.Level.ERROR)
    sc._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark").setLevel(sc._jvm.org.apache.log4j.Level.ERROR)

    engine = PipelineEngine(spark, user_json_str)

    # Register plugins
    engine.register_reader("csv_batch", MockCSVReader())
    engine.register_writer("csv_batch", MockCSVWriter())

    # --- STEP D: Execution ---
    print("🚀 STARTING PIPELINE TEST...")
    try:
        engine.run()
        print("\n✅ TEST PASSED SUCCESSFULLY")
        print(f"👉 Check your results here: {abs_output_path}")

        # Verify the business token was actually applied in the output
        result_df = spark.read.option("header", "true").csv(os.path.join(abs_output_path, "part-*.csv"))
        print("\n--- VERIFYING DATA CONTENT ---")
        result_df.show()

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup (Optional: Comment out if you want to inspect files manually!)
        print("\n🧹 Cleaning up temporary test files...")
        if os.path.exists(input_rel_path):
            os.remove(input_rel_path)
        if os.path.exists(output_rel_dir):
            shutil.rmtree(output_rel_dir)
        spark.stop()
        print("Cleanup complete.")


if __name__ == "__main__":
    run_test()
