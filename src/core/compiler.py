import json
from jinja2 import Environment, BaseLoader
from .models import PipelineConfig  # Assuming models is in framework_core


class CodeGenerator:
    def __init__(self, config_json: str):
        # Parse the configuration
        self.config = PipelineConfig.parse_raw(config_json)

        # Setup Jinja Environment
        self.jinja_env = Environment(loader=BaseLoader())

        # 1. REGISTER CORE FRAMEWORK FUNCTIONS (Internal)
        self.jinja_env.globals['ref'] = lambda x: x

        # 2. REGISTER USER BUSINESS TOKENS (External)
        # We merge the user's tokens into the template context
        self.tokens = self.config.token_dict
        self.jinja_env.globals.update(self.tokens)

    def _render(self, text: str) -> str:
        """Resolves all Jinja templates (ref and user tokens)."""
        if not text or not isinstance(text, str):
            return text
        return self.jinja_env.from_string(text).render()

    def generate(self) -> str:
        """Generates a standalone, runnable Python script."""
        lines = [
            "import pyspark.sql.functions as F",
            "from pyspark.sql import SparkSession",
            "spark = SparkSession.builder.appName('Generated_Pipeline').getOrCreate()\n"
        ]

        # 1. Process Readers
        lines.append("# --- STEP 1: READERS ---")
        for r_id, r_cfg in self.config.reader.items():
            path = self._render(r_cfg.path) if r_cfg.path else ""
            lines.append(f"# Reader ID: {r_id}")
            lines.append(f"df_{r_id} = spark.read.option('header', {str(r_cfg.header).lower()}).csv('{path}')")
            lines.append(f"spark.createOrReplaceTempView('{r_cfg.view_name}')\n")

        # 2. Process Transformations
        lines.append("# --- STEP 2: TRANSFORMATIONS ---")
        for t_id, t_cfg in self.config.transformation.items():
            lines.append(f"# Transformation ID: {t_id}")

            # Handle both 'sql' and 'sql_step' types from JSON
            if t_cfg.type in ['sql', 'sql_step']:
                query = self._render(t_cfg.query)
                lines.append(f"spark.sql('''{query}''').createOrReplaceTempView('{t_cfg.view_name}')")

            elif t_cfg.type == 'python_step':
                if not t_cfg.function_path:
                    raise ValueError(f"Python step {t_id} missing function_path")
                module_path, func_name = t_cfg.function_path.split('.')
                lines.append(f"import {module_path}")
                lines_args = ", ".join([f"'{v}'" for v in t_cfg.input_views])
                lines.append(
                    f"df_{t_id} = {module_path}.{func_name}(spark, {f'[{args}]' if args else ''}, context={{}})\n")
                lines.append(f"spark.createOrReplaceTempView('{t_cfg.view_name}')")

            lines.append("")

        # 3. Process Writers
        lines.append("# --- STEP 3: WRITERS ---")
        for w_id, w_cfg in self.config.writer.items():
            path = self._render(w_cfg.path)
            lines.append(f"# Writer ID: {w_id}")
            lines.append(f"df_write_{w_id} = spark.table('{w_cfg.write_view_name}')")
            writer = df_write_w_id_placeholder = f"df_write_{w_id}.write.mode('{w_cfg.mode}').save('{path}')"
            if w_cfg.partition_by:
                partitions = ", ".join([f"'{p}'" for p in w_cfg.partition_by])
                writer += f".partitionBy({partitions})"
            lines.append(writer)
            lines.append("")

        return "\n".join(lines)
