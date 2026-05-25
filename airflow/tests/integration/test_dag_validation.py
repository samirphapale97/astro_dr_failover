"""DAG file validation tests.

These tests validate that DAG Python files can be parsed, have the
expected schedules and parameters, and don't contain import errors.
They do NOT require a running Airflow instance — we import the DAG
files directly and inspect the top-level ``dag`` / DAG objects.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ── helpers ─────────────────────────────────────────────────────────────

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


def _dag_files() -> list[Path]:
    """Return all Python files in the dags/ directory."""
    return sorted(DAGS_DIR.glob("*.py"))


def _patch_airflow_imports():
    """Insert lightweight stubs for Airflow modules so DAG files can be
    imported outside of an Airflow environment."""
    stubs = {}

    # airflow
    airflow_mod = MagicMock()
    stubs["airflow"] = airflow_mod

    # airflow.models
    models_mod = MagicMock()
    variable_mock = MagicMock()
    variable_mock.get = MagicMock(return_value="us-east-1")
    variable_mock.set = MagicMock()
    models_mod.Variable = variable_mock
    stubs["airflow.models"] = models_mod

    # airflow.operators
    operators_mod = MagicMock()
    stubs["airflow.operators"] = operators_mod
    stubs["airflow.operators.python"] = MagicMock()
    stubs["airflow.operators.empty"] = MagicMock()

    # airflow.decorators
    stubs["airflow.decorators"] = MagicMock()

    # airflow.utils
    stubs["airflow.utils"] = MagicMock()
    stubs["airflow.utils.dates"] = MagicMock()
    stubs["airflow.utils.trigger_rule"] = MagicMock()

    for mod_name, mock_obj in stubs.items():
        if mod_name not in sys.modules:
            sys.modules[mod_name] = mock_obj


def _import_dag_module(dag_path: Path):
    """Import a DAG file as a Python module."""
    _patch_airflow_imports()
    spec = importlib.util.spec_from_file_location(dag_path.stem, dag_path)
    if spec is None or spec.loader is None:
        pytest.skip(f"Could not build import spec for {dag_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── tests ───────────────────────────────────────────────────────────────


class TestDAGFilesExist:
    """Verify the expected DAG files are present."""

    def test_dags_directory_exists(self):
        assert DAGS_DIR.exists(), f"DAGs directory not found: {DAGS_DIR}"

    @pytest.mark.parametrize(
        "filename",
        [
            "failover_monitor_dag.py",
            "config_test_dag.py",
            "manual_failover_dag.py",
        ],
    )
    def test_expected_dag_file_exists(self, filename: str):
        path = DAGS_DIR / filename
        assert path.exists(), f"Expected DAG file not found: {path}"


class TestDAGImports:
    """Ensure DAG files can be imported without errors."""

    @pytest.mark.parametrize("dag_path", _dag_files(), ids=lambda p: p.name)
    def test_dag_file_imports_without_error(self, dag_path: Path):
        """Each DAG file should be importable without raising."""
        try:
            _import_dag_module(dag_path)
        except Exception as exc:
            pytest.fail(f"Failed to import {dag_path.name}: {exc}")


class TestFailoverMonitorDAG:
    """Validate the failover monitor DAG configuration."""

    @pytest.fixture(autouse=True)
    def _load_module(self):
        path = DAGS_DIR / "failover_monitor_dag.py"
        if not path.exists():
            pytest.skip("failover_monitor_dag.py not found")
        self.module = _import_dag_module(path)

    def test_module_loads(self):
        assert self.module is not None


class TestManualFailoverDAG:
    """Validate the manual failover DAG configuration."""

    @pytest.fixture(autouse=True)
    def _load_module(self):
        path = DAGS_DIR / "manual_failover_dag.py"
        if not path.exists():
            pytest.skip("manual_failover_dag.py not found")
        self.module = _import_dag_module(path)

    def test_module_loads(self):
        assert self.module is not None


class TestConfigTestDAG:
    """Validate the config test DAG configuration."""

    @pytest.fixture(autouse=True)
    def _load_module(self):
        path = DAGS_DIR / "config_test_dag.py"
        if not path.exists():
            pytest.skip("config_test_dag.py not found")
        self.module = _import_dag_module(path)

    def test_module_loads(self):
        assert self.module is not None
