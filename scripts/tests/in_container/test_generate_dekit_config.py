# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml

_GENERATOR = Path(__file__).parents[2] / "in_container" / "bin" / "generate_dekit_config.py"
_spec = importlib.util.spec_from_file_location("generate_dekit_config", _GENERATOR)
assert _spec is not None
assert _spec.loader is not None
generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generator)

_COMPONENT_ENV_VARS = (
    "USE_AIRFLOW_VERSION",
    "INTEGRATION_CELERY",
    "CELERY_FLOWER",
    "STANDALONE_DAG_PROCESSOR",
    "AIRFLOW__CORE__EXECUTOR",
    "DEV_MODE",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in _COMPONENT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _config(cwd: str = "/opt/airflow") -> dict:
    return yaml.safe_load(generator.generate_dekit_config(cwd=cwd))


def test_every_component_is_an_autostarted_task_run_through_bash():
    config = _config()

    assert config["defaults"] == {"cwd": "/opt/airflow", "autorestart": "always", "scrollback_len": 100000}
    assert list(config["tasks"]) == ["scheduler", "api_server", "triggerer", "shell"]
    for task in config["tasks"].values():
        assert task["autostart"] is True
        assert task["cmd"][:2] == ["bash", "-c"]
    assert config["tasks"]["scheduler"]["cmd"][2] == "airflow scheduler"
    assert config["tasks"]["shell"]["cmd"][2] == "bash"


def test_optional_components_follow_the_environment(monkeypatch):
    monkeypatch.setenv("INTEGRATION_CELERY", "true")
    monkeypatch.setenv("CELERY_FLOWER", "true")
    monkeypatch.setenv("STANDALONE_DAG_PROCESSOR", "true")
    monkeypatch.setenv(
        "AIRFLOW__CORE__EXECUTOR", "airflow.providers.edge3.executors.edge_executor.EdgeExecutor"
    )

    tasks = _config()["tasks"]

    assert {"celery_worker", "flower", "edge_worker", "dag_processor"} <= set(tasks)
    # The edge worker unsets variables first, which needs the shell.
    assert "unset AIRFLOW__DATABASE__SQL_ALCHEMY_CONN || true &&" in tasks["edge_worker"]["cmd"][2]


def test_airflow_2_runs_the_webserver(monkeypatch):
    monkeypatch.setenv("USE_AIRFLOW_VERSION", "2.11.0")

    tasks = _config()["tasks"]

    assert "webserver" in tasks
    assert "api_server" not in tasks


def test_no_mprocs_keys_are_left():
    config = _config()

    assert "procs" not in config
    for task in config["tasks"].values():
        assert not {"shell", "restart", "scrollback"} & set(task)
