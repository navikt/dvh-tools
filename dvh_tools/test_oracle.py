import os
from unittest.mock import Mock

import pytest

from dvh_tools import oracle


@pytest.fixture
def connect(monkeypatch):
    for name in tuple(os.environ):
        if name.startswith(("DB_", "DBT_")):
            monkeypatch.delenv(name)
    connection_mock = Mock()
    monkeypatch.setattr(oracle.oracledb, "connect", connection_mock)
    return connection_mock


@pytest.mark.parametrize("target", [None, "prod", "knast", " KNAST "])
def test_connection_selection(monkeypatch, connect, target):
    if target is not None:
        monkeypatch.setenv("DBT_DB_TARGET", target)
    monkeypatch.setenv("DBT_ENV_SECRET_USER", "knast_user")
    monkeypatch.setenv("DBT_DB_HOST", "knast_host")
    monkeypatch.setenv("DBT_DB_PORT", "1521")
    monkeypatch.setenv("DBT_ENV_SERVICE", "knast_service")
    monkeypatch.setenv("DB_USER", "legacy_user")
    monkeypatch.setenv("DB_PASSWORD", "legacy_password")

    assert oracle._create_connection() is connect.return_value

    if target == "prod":
        connect.assert_called_once_with(user="legacy_user", password="legacy_password", dsn=None)
    else:
        connect.assert_called_once_with(user="knast_user", dsn="knast_host:1521/knast_service")


@pytest.mark.parametrize("use_secret", [False, True])
def test_existing_credentials_take_precedence(monkeypatch, connect, use_secret):
    monkeypatch.setenv("DBT_ENV_SECRET_USER", "override_user")
    monkeypatch.setenv("DBT_ENV_SECRET_PASS", "override_password")
    monkeypatch.setenv("DBT_DB_HOST", "knast_host")
    monkeypatch.setenv("DBT_DB_PORT", "1521")
    monkeypatch.setenv("DBT_ENV_SERVICE", "knast_service")
    secret = {"DB_USER": "secret_user", "DB_PASSWORD": "secret_password", "DB_DSN": "secret_dsn"}
    if use_secret:
        oracle._create_connection(secret)
        connect.assert_called_once_with(user="override_user", password="override_password", dsn="secret_dsn")
    else:
        monkeypatch.setenv("DB_USER", "legacy_user")
        monkeypatch.setenv("DB_PASSWORD", "legacy_password")
        monkeypatch.setenv("DB_DSN", "legacy_dsn")
        oracle._create_connection()
        connect.assert_called_once_with(user="legacy_user", password="legacy_password", dsn="legacy_dsn")


def test_secret_without_environment_overrides(connect):
    secret = {"DB_USER": "secret_user", "DB_PASSWORD": "secret_password", "DB_DSN": "secret_dsn"}
    oracle._create_connection(secret)
    connect.assert_called_once_with(user="secret_user", password="secret_password", dsn="secret_dsn")


def test_explicit_knast_ignores_secret(monkeypatch, connect):
    monkeypatch.setenv("DBT_DB_TARGET", "knast")
    monkeypatch.setenv("DBT_ENV_SECRET_USER", "knast_user")
    monkeypatch.setenv("DBT_DB_HOST", "knast_host")
    monkeypatch.setenv("DBT_DB_PORT", "1521")
    monkeypatch.setenv("DBT_ENV_SERVICE", "knast_service")
    oracle._create_connection({"DB_USER": "secret_user", "DB_PASSWORD": "secret_password", "DB_DSN": "secret_dsn"})
    connect.assert_called_once_with(user="knast_user", dsn="knast_host:1521/knast_service")


def test_incomplete_knast_configuration(monkeypatch, connect):
    monkeypatch.setenv("DBT_DB_TARGET", "knast")
    with pytest.raises(ValueError, match="Missing Knast connection settings: DBT_ENV_SECRET_USER"):
        oracle._create_connection()
    connect.assert_not_called()


def test_partial_knast_settings_do_not_trigger_detection(monkeypatch, connect):
    monkeypatch.setenv("DBT_ENV_SECRET_USER", "knast_user")
    monkeypatch.setenv("DBT_DB_HOST", "knast_host")
    oracle._create_connection()
    connect.assert_called_once_with(user=None, password=None, dsn=None)


def test_connection_errors_are_not_retried(connect):
    connect.side_effect = RuntimeError("Connection failed")
    with pytest.raises(RuntimeError, match="Connection failed"):
        oracle._create_connection()
    connect.assert_called_once()