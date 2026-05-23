import json
import os
from MetaFlux.src.core.compiler import CodeGenerator

def run_test():
    # 1. The User's JSON Configuration (exactly as provided)
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

    print("🚀 Starting CodeGenerator Test...")

    try:
        # 2. Initialize Generator
        generator = CodeGenerator(user_json_str)

        # 3. Generate the code
        generated_code = generator.generate()

        # 4. VERIFICATION LOGIC
        print("\n" + "="*50)
        print("GENERATED PYTHON CODE:")
        print("="*50)
        print(generated_code)
        print("="*50)

        # Verification Check 1: Does the SQL contain the resolved project name?
        if "'grocery_analytics'" in generated_code:
            print("✅ SUCCESS: User token 'project_name' was correctly injected.")
        else:
            print("❌ FAILED: User token injection failed.")

        # Verification Check 2: Is the ref() function resolved to a plain string?
        if "FROM vw_gro_transformed" in generated_code or "FROM vw_groceries" in generated_code:
             # Note: In our code, ref returns the name itself.
             print("✅ SUCCESS: 'ref()' function was correctly expanded.")
        else:
            print("❌ FAILED: Jinja 'ref' expansion failed.")

        # Verification Check 3: Is the writer path correct?
        if "test_output_folder" in generated_code:
            print("✅ SUCCESS: Writer path is correct.")
        else:
            print("❌ FAILED: Writer path resolution failed.")

        print("\n✨ TEST COMPLETE ✨")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_test()
