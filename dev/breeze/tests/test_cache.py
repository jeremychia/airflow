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

from pathlib import Path
from unittest import mock

import click
import pytest
from click.testing import CliRunner

from airflow_breeze.commands.common_options import option_terminal_multiplexer
from airflow_breeze.global_constants import (
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    ALLOWED_TERMINAL_MULTIPLEXERS,
    TERMINAL_MULTIPLEXER_ALIASES,
)
from airflow_breeze.utils.cache import (
    check_if_cache_exists,
    check_if_values_allowed,
    delete_cache,
    read_from_cache_file,
    resolve_value_alias,
)

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()


@pytest.mark.parametrize(
    ("parameter", "value", "result", "exception"),
    [
        ("backend", "mysql", (True, ["sqlite", "mysql", "postgres", "none", "custom"]), None),
        ("backend", "xxx", (False, ["sqlite", "mysql", "postgres", "none", "custom"]), None),
        ("python_major_minor_version", "3.10", (True, ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS), None),
        ("missing", "value", None, AttributeError),
    ],
)
def test_allowed_values(parameter, value, result, exception):
    if exception:
        with pytest.raises(expected_exception=exception):
            check_if_values_allowed(parameter, value)
    else:
        assert result == check_if_values_allowed(parameter, value)


@mock.patch("airflow_breeze.utils.cache.Path")
def test_check_if_cache_exists(path):
    check_if_cache_exists("test_param")
    path.assert_called_once_with(AIRFLOW_SOURCES / ".build")


@pytest.mark.parametrize(
    "param",
    [
        "test_param",
        "mysql_version",
        "executor",
    ],
)
def test_read_from_cache_file(param):
    param_value = read_from_cache_file(param.upper())
    if param_value is None:
        assert param_value is None
    else:
        allowed, param_list = check_if_values_allowed(param, param_value)
        if allowed:
            assert param_value in param_list


@mock.patch("airflow_breeze.utils.cache.Path")
@mock.patch("airflow_breeze.utils.cache.check_if_cache_exists")
def test_delete_cache_exists(mock_check_if_cache_exists, mock_path):
    param = "MYSQL_VERSION"
    mock_check_if_cache_exists.return_value = True
    cache_deleted = delete_cache(param)
    mock_path.assert_called_with(AIRFLOW_SOURCES / ".build")
    assert cache_deleted


@mock.patch("airflow_breeze.utils.cache.Path")
@mock.patch("airflow_breeze.utils.cache.check_if_cache_exists")
def test_delete_cache_not_exists(mock_check_if_cache_exists, mock_path):
    param = "TEST_PARAM"
    mock_check_if_cache_exists.return_value = False
    cache_deleted = delete_cache(param)
    assert not cache_deleted


@pytest.mark.parametrize(
    ("param_name", "value", "expected"),
    [
        ("TERMINAL_MULTIPLEXER", "mprocs", "dekit"),
        ("TERMINAL_MULTIPLEXER", "dekit", "dekit"),
        ("TERMINAL_MULTIPLEXER", "tmux", "tmux"),
        ("BACKEND", "mprocs", "mprocs"),
    ],
)
def test_resolve_value_alias(param_name, value, expected):
    assert resolve_value_alias(param_name, value) == expected


def test_mprocs_is_not_offered_as_a_choice():
    assert "mprocs" not in ALLOWED_TERMINAL_MULTIPLEXERS
    assert TERMINAL_MULTIPLEXER_ALIASES["mprocs"] in ALLOWED_TERMINAL_MULTIPLEXERS


def test_cached_old_name_is_migrated(monkeypatch, tmp_path):
    monkeypatch.setattr("airflow_breeze.utils.cache.BUILD_CACHE_PATH", tmp_path)
    (tmp_path / ".TERMINAL_MULTIPLEXER").write_text("mprocs")

    assert read_from_cache_file("TERMINAL_MULTIPLEXER") == "dekit"
    assert (tmp_path / ".TERMINAL_MULTIPLEXER").read_text() == "dekit"


def test_cached_current_value_is_left_alone(monkeypatch, tmp_path):
    monkeypatch.setattr("airflow_breeze.utils.cache.BUILD_CACHE_PATH", tmp_path)
    (tmp_path / ".TERMINAL_MULTIPLEXER").write_text("tmux")

    assert read_from_cache_file("TERMINAL_MULTIPLEXER") == "tmux"
    assert (tmp_path / ".TERMINAL_MULTIPLEXER").read_text() == "tmux"


@pytest.mark.parametrize("given", ["mprocs", "dekit"])
def test_option_accepts_the_old_name_and_remembers_the_new_one(monkeypatch, tmp_path, given):
    monkeypatch.setattr("airflow_breeze.utils.cache.BUILD_CACHE_PATH", tmp_path)
    monkeypatch.delenv("SKIP_SAVING_CHOICES", raising=False)

    @click.command()
    @option_terminal_multiplexer
    def command(terminal_multiplexer):
        click.echo(terminal_multiplexer)

    result = CliRunner().invoke(command, ["--terminal-multiplexer", given])

    assert result.exit_code == 0, result.output
    assert result.output.strip().splitlines()[-1] == "dekit"
    assert (tmp_path / ".TERMINAL_MULTIPLEXER").read_text() == "dekit"
