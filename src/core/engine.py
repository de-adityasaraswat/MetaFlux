import importlib
import os
from datetime import datetime, timedelta
from graphlib import TopologicalSorter
from jinja2 import Environment, BaseLoader
from .models import PipelineConfig
from .logger import get_logger, trace_execution
import inspect
from .interfaces import AbstractReader, AbstractWriter

class PipelineEngine:
    def __init__(self, spark, config_json: str):
        self.spark = spark
        self.config = PipelineConfig.parse_raw(config_json)
        self.tokens = self.config.token_dict
        self.logger = get_logger()

        # 1. Setup the Jinja Environment
        self.jinja_env = Environment(loader=BaseLoader())

        # 2. Register CORE Framework Functions (Internal)
        self._setup_internal_functions()

        # 3. Register USER Business Tokens (External)
        # This merges user tokens (like project_name) into the environment
        self.jinja_env.globals.update(self.tokens)

        # Automatic Plugin Discovery
        self._readers = {}
        self._writers = {}
        self._discover_plugins()

    def _discover_plugins(self):
        """
        The Magic: Automatically scans modules and registers classes
        that inherit from AbstractReader or AbstractWriter.
        """
        from . import reader_plugins, writer_plugins  # Use relative imports within the package

        # We get the absolute path of the current module (engine.py)
        # and identify its parent package name (e. e., 'framework_core')
        current_package = __package__
        if not current_package:
            # Fallback for running as a standalone script
            current_package = os.path.dirname(self.__class__.__module__)

        search_targets = [
            f"{current_package}.reader_plugins",
            f"{current_package}.writer_plugins"
        ]

        for module_path in search_targets:
            try:
                # Dynam_ically import the discovered path
                module = importlib.import_module(module_path)
                self.logger.info(f"Successfully loaded plugin module: {module_path}")

                for name, obj in inspect.getmembers(module, inspect.isclass):
                    # Check if it's a Reader
                    if issubclass(obj, AbstractReader) and obj is not AbstractReader:
                        key = name.lower().replace('reader', '').strip('_')
                        self._readers[key] = obj()
                        self.logger.info(f"Found Reader Plugin: {name} ({key})")

                    # Check if it's a Writer
                    if issubclass(obj, AbstractWriter) and obj is not AbstractWriter:
                        key = name.lower().replace('writer', '').strip('_')
                        self._writers[key] = obj()
                        self.logger.info(f"Found Writer Plugin: {name} ({key})")

            except Exception as e:
                # If a module doesn'_t exist (e.g. if user hasn't added writer_plugins), we log and move on
                self.logger.error(f"Could not load plugins from {module_path}: {e}")

    def _setup_internal_functions(self):
        """
        Defines internal functions that are part of the framework
        and NOT provided by the user in JSON.
        """
        # The ref function for dependency resolution
        self.jinja_env.globals['ref'] = lambda x: x

        # --- AIRFLOW-LIKE DATE UTILITIES ---
        # These allow users to use {{ today() }}, {{ yesterday() }} in SQL
        self.jinja_env.globals['today'] = lambda: datetime.now().strftime('%Y-%m-%d')
        self.jinja_env.globals['yesterday'] = lambda: (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.jinja_env.globals['last_7_days'] = lambda: (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        self_date_format = '%Y-%m-%d %H:%M:%S'
        self.jinja_env.globals['now'] = lambda: datetime.now().strftime(self_date_format)

    def register_reader(self,name: str, handler):
        self._readers[name] = handler

    def register_writer(self, type_name: str, handler):
        self._writers[type_name] = handler

    @trace_execution
    def run(self):
        self.logger.info("Starting Pipeline Execution")
        self._validate_preflight()

        # 1. Build the DAG (Topological Sort)
        ts = TopologicalSorter()

        # Add Readers as base nodes (they have no dependencies)
        for r_cfg in self.config.reader.values():
            ts.add(r_sing_name := r_cfg.view_name)
        # Add Transformations and their dependency edges
        for t_id, t_cfg in self.config.transformation.items():
            if t_cfg.depends_on:
                ts.add(t_cfg.view_name, *t_cfg.depends_on)
            else:
                ts.add(t_cfg.view_name)

        # The execution order is a flat list of views to be processed
        execution_order = list(ts.static_order())
        self.logger.info(f"Execution Plan determined: {execution_order}")

        # 2. Execution Loop
        for current_view in execution_order:

            # --- ROLE 1: Is this view a Reader? ---
            reader_match = next((r for r in self.config.reader.values()
                                 if r.view_name == current_view), None)
            if reader_match:
                self._run_reader_step(reader_match)
                # We 'continue' here because a Reader is the START of a node.
                # It doesn't depend on anything else in the loop.
                continue

            # --- ROLE 2: Is this view a Transformation? ---
            trans_match = next((t for t_id, t in self.config.transformation.items()
                                if t.view_name == current_view), None)
            if trans_match:
                self._run_transformation_step(trans_match)
                # We DO NOT 'continue' here.
                # Why? Because this same view might also be a Writer target!

            # --- ROLE 3: Is this view a Writer destination? ---
            writer_match = next((w for w in self.config.writer.values()
                                 if w.write_view_name == current_view), None)
            if writer_match:
                self._run_writer_step(writer_match)

        self.logger.info("Pipeline Execution Completed Successfully.")

    def _validate_preflight(self):
        """Checks for path existence and module availability."""
        for r_id, r_cfg in self.config.reader.items():
            if r_cfg.path and not os.path.exists(r_cfg.path) and "test" not in r_cfg.path:
                raise FileNotFoundError(f"Path error for {r_id}: {r_cfg.path}")

    def _run_reader_step(self, r_cfg):
        handler = self._readers.get(r_cfg.type)
        if not handler:
            raise ValueError(f"No reader registered for type: {r_cfg.type}")
        self.logger.info(f"Executing Reader: {r_cfg.view_name}")
        df = handler.read(self.spark, r_cfg, self.tokens)
        df.createOrReplaceTempView(r_cfg.view_name)

    def _run_transformation_step(self, t_cfg):
        if t_cfg.type == 'sql':
            self.logger.info(f"Executing SQL Transformation: {t_cfg.view_name}")
            # Use the engine's environment to render templates (handles ref + tokens)
            template = self.jinja_env.from_string(t_cfg.query)
            rendered_sql = template.render()
            self.spark.sql(rendered_sql).createOrReplaceTempView(t_cfg.view_name)

        elif t_cfg.type == 'python':
            self.logger.info(f"Executing Python Transformation: {t_cfg.view_name}")
            if not t_cfg.function_path:
                raise ValueError("python_step requires a function_path")

            module_path, func_name = t_cfg.function_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            user_func = getattr(module, func_name)

            input_dfs = [self.spark.table(v) for v in t_cfg.input_views]

            if len(input_dfs) == 1:
                result_df = user_func(self.spark, input_dfs[0], self.tokens)
            else:
                result_df = user_func(self.spark, *input_dfs, self.tokens)
            result_df.createOrReplaceTempView(t_cfg.view_name)

    def _run_writer_step(self, w_cfg):
        handler = self._writers.get(w_cfg.type)
        if not handler:
            raise ValueError(f"No writer registered for type: {w_cfg.type}")
        self.logger.info(f"Executing Writer: {w_cfg.write_view_name}")
        handler.write(self.spark, w_cfg, self.config.transformation)
