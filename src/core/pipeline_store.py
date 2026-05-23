from typing import List, Any
import json
import os
from compiler import CodeGenerator


class PipelineStateStore:
    def __init__(self, registry_file: str):
        self.registry_file = registry_file
        self._ensure_store_exists()

    def _ensure_store_exists(self):
        if not os.path.exists(self.registry_file):
            with open(self.registry_file, 'w') as f:
                json.dump({}, f)

    # FIXED: Changed *args to List[str] for proper type hinting
    def _generate_key(self, src_info: str, dest_info: List[str]) -> str:
        """
        Creates a unique lookup key from source and destination parts.
        Example input:
            src_info="sales_db.raw"
            dest_info=["gold_db", "refined", "orders"]
        Result: "sales_db_raw_gold_db_refined_orders"
        """
        # Clean source info (remove colons/slashes)
        clean_src = src_int_sanitized = src_info.replace(":", "_").replace("/", "_").replace("\\", "_")

        # Join all destination parts with underscores
        clean_dest = "_".join(dest_info).replace(":", "_").replace("/", "_").replace("\\", "_")

        return f"{clean_src}_{clean_dest}"

    def register_pipeline(self, src_info: str, dest_info: List[str], config_json: str) -> str:
        """
        API: Register a pipeline.
        :param src_info: string (e.g., 'prod.raw')
        :param dest_info: list of strings (e.g., ['prod', 'gold', 'table'])
        :param config_json: the raw JSON string
        """
        # Call the fixed key generator
        key = self._generate_key(src_info, dest_info)

        with open(self.registry_file, 'r+') as f:
            data = json.load(f)
            data[key] = json.loads(config_json)
            f.seek(0)
            json.dump(data, f, indent=4)
            f.truncate()
        return key

    def get_generated_script(self, key: str, generator_class: Any) -> str:
        """API: Retrieves config and uses Generator class to build the script."""
        with open(self.registry_file, 'r') as f:
            data = json.load(f)
            if key not in data:
                raise KeyError(f"Key {key} not found in registry.")

            config_dict = data[key]
            # Re-serialize to JSON string because Generator expects a JSON string
            config_json_str = json.dumps(config_dict)

            # Instantiate the class and call its generate method
            generator_instance = generator_class(config_json_str)
            return generator_instance.generate_script()


# ==========================================
# TEST EXECUTION (To prove it works)
# ==========================================
if __name__ == "__main__":
    # Mocking a user-provided JSON
    user_json = """
    {
      "reader": { "r1": { "type": "csv_batch", "path": "data.csv", "view_name": "v1" } },
      "transformation": { "t1": { "type": "sql_step", "query": "select * from {{ ref('v1') }}", "view_name": "v2", "depends_on": ["v1"] }},
      "writer": { "w1": { "write_view_name": "v2", "type": "delta_batch", "path": "out", "mode": "append" }},
      "tokens": [{"name": "project", "value": "demo"}]
    }
    """

    # 1. Initialize Store and Generator (from previous module)
    store = PipelineStateStore("registry_test.json")

    # Note: We assume CodeGenerator exists from our previous conversation
    # from framework_core.compiler import CodeGenerator

    # 2. Register a pipeline
    new_key = store.register_pipeline("src_db.schema.table", ["dest_db", "dest_sch", "out_table"], user_json)
    print(f"Successfully registered key: {new_key}")

    # 3. Generate the code for that key
    try:
        # We pass the class 'CodeGenerator', not an instance, as per our API design
        script = store.get_generated_script(new_key, CodeGenerator)
        print("\n--- GENERATED SCRIPT FOR KEY ---")
        print(script)
    except Exception as e:
        print(f"Error: {e}")
