"""Tests for the mercado_publico DAG."""
import json
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk import TriggerRule
from airflow.sdk.exceptions import AirflowSkipException

from dags import mercado_publico as mercado_publico_module

GROUP = "process_licitaciones"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="dags/")
    assert "mercado_publico" in bag.dags, (
        f"mercado_publico DAG not loaded. Import errors: {bag.import_errors}"
    )
    assert not bag.import_errors, bag.import_errors
    return bag.dags["mercado_publico"]


EXPECTED_TASKS = {
    "get_licitaciones",
    f"{GROUP}.get_licitacion_to_process",
    f"{GROUP}.wait_before_process",
    f"{GROUP}.process_licitacion",
    f"{GROUP}.prepare_licitacion",
    f"{GROUP}.prepare_licitacion_dlq",
    f"{GROUP}.upsert_licitacion",
    f"{GROUP}.upsert_licitacion_dlq",
    f"{GROUP}.iteration_finished",
    f"{GROUP}.evaluate_loop",
    "finished",
}


class TestDAGStructure:
    def test_dag_loads(self, dag):
        assert dag is not None

    def test_dag_has_expected_tasks(self, dag):
        assert {t.task_id for t in dag.tasks} == EXPECTED_TASKS

    def test_dag_schedule(self, dag):
        assert str(dag.timetable.expression) in ("@daily", "0 0 * * *")

    def test_dag_tags(self, dag):
        assert "scrapping" in dag.tags
        assert "mercadopublico" in dag.tags

    def test_dag_default_args(self, dag):
        assert dag.default_args["retries"] == 1
        assert dag.default_args["owner"] == "emartinez"

    def test_detail_task_uses_pool(self, dag):
        assert dag.get_task(f"{GROUP}.process_licitacion").pool == "mp_licitacion_detail_pool"

    def test_wait_sensor_delays_the_api_call(self, dag):
        sensor = dag.get_task(f"{GROUP}.wait_before_process")
        assert sensor.time_to_wait == timedelta(
            seconds=mercado_publico_module.API_CALL_DELAY_SECONDS
        )
        assert sensor.deferrable is True
        assert f"{GROUP}.process_licitacion" in sensor.downstream_task_ids
        assert f"{GROUP}.wait_before_process" in dag.get_task(
            f"{GROUP}.process_licitacion"
        ).upstream_task_ids

    def test_upserts_are_mapped_from_the_right_guard(self, dag):
        for task_id, source_task_id in (
            ("upsert_licitacion", "prepare_licitacion"),
            ("upsert_licitacion_dlq", "prepare_licitacion_dlq"),
        ):
            task = dag.get_task(f"{GROUP}.{task_id}")
            assert task.is_mapped
            assert task.expand_input.value["parameters"][0].operator.task_id == (
                f"{GROUP}.{source_task_id}"
            )
            assert task.downstream_task_ids == {f"{GROUP}.iteration_finished"}

    def test_loop_has_single_terminal_feeding_the_gate(self, dag):
        group = dag.task_group_dict[GROUP]
        assert [t.task_id for t in group.get_leaves()] == [f"{GROUP}.evaluate_loop"]
        assert dag.get_task(f"{GROUP}.iteration_finished").downstream_task_ids == {
            f"{GROUP}.evaluate_loop"
        }

    def test_finished_runs_after_the_loop(self, dag):
        assert dag.get_task(f"{GROUP}.evaluate_loop").downstream_task_ids == {"finished"}


class TestSkipGuardsAndTerminal:
    def _python_callable(self, dag, task_id):
        return dag.get_task(f"{GROUP}.{task_id}").python_callable

    def test_prepare_licitacion_skips_error_records(self, dag):
        fn = self._python_callable(dag, "prepare_licitacion")
        with pytest.raises(AirflowSkipException):
            fn({"codigo_externo": "123", "is_error": True})

    def test_prepare_licitacion_serializes_jsonb_fields(self, dag):
        fn = self._python_callable(dag, "prepare_licitacion")
        result = fn(
            {
                "codigo_externo": "123",
                "comprador": {"nombre": "Órgano"},
                "adjudicacion": {"fecha": "2026-01-01"},
                "items": [{"id": 1}],
                "fechas": {"fecha_cierre": "2026-01-02"},
            }
        )
        for field in ("comprador", "adjudicacion", "items", "fechas"):
            assert isinstance(result[field], str)
            assert json.loads(result[field])
        assert json.loads(result["comprador"]) == {"nombre": "Órgano"}
        assert result["codigo_externo"] == "123"

    def test_prepare_licitacion_handles_missing_jsonb_fields(self, dag):
        fn = self._python_callable(dag, "prepare_licitacion")
        result = fn({"codigo_externo": "123"})
        assert json.loads(result["comprador"]) == {}
        assert json.loads(result["fechas"]) == {}

    def test_prepare_licitacion_dlq_skips_valid_records(self, dag):
        fn = self._python_callable(dag, "prepare_licitacion_dlq")
        with pytest.raises(AirflowSkipException):
            fn({"codigo_externo": "123"})

    def test_prepare_licitacion_dlq_returns_the_sql_keys(self, dag):
        fn = self._python_callable(dag, "prepare_licitacion_dlq")
        result = fn(
            {
                "codigo_externo": "123",
                "error_message": "boom",
                "payload": "<html/>",
                "is_error": True,
            }
        )
        assert result == {
            "codigo_externo": "123",
            "error_message": "boom",
            "payload": "<html/>",
        }

    def test_prepare_licitacion_dlq_fills_missing_keys(self, dag):
        fn = self._python_callable(dag, "prepare_licitacion_dlq")
        assert fn({"codigo_externo": "123", "is_error": True}) == {
            "codigo_externo": "123",
            "error_message": "",
            "payload": "",
        }

    def test_api_session_retries_rate_limits_with_backoff(self):
        session = mercado_publico_module._create_api_session()
        retries = session.get_adapter("https://api.mercadopublico.cl").max_retries
        assert 429 in retries.status_forcelist
        assert retries.backoff_factor == 1
        assert retries.respect_retry_after_header is True
        assert "GET" in retries.allowed_methods

    def test_process_licitacion_captures_connection_errors(self, dag):
        fn = self._python_callable(dag, "process_licitacion")
        # DagBag carga el módulo con otro nombre, así que se parchea el globals
        # de la propia función para no llamar a la API real.
        session_factory = MagicMock()
        session_factory.return_value.get.side_effect = RuntimeError("connection lost")
        with (
            patch.dict(fn.__globals__, {"_create_api_session": session_factory}),
            patch("airflow.sdk.Variable.get", return_value="ticket"),
        ):
            result = fn({"current_licitacion": "123"})
        session_factory.return_value.get.assert_called_once_with(
            mercado_publico_module.ENDPOINT_LICITACION,
            params={"codigo": "123", "ticket": "ticket"},
            timeout=30,
        )
        assert result["is_error"] is True
        assert result["codigo_externo"] == "123"
        assert "connection lost" in result["error_message"]
        assert result["payload"] == ""

    def test_terminal_returns_remaining_licitaciones(self, dag):
        fn = self._python_callable(dag, "iteration_finished")
        assert fn({"next_licitaciones": ["a", "b"]}) == {"next_licitaciones": ["a", "b"]}
        assert fn({}) == {"next_licitaciones": []}

    def test_terminal_allows_the_skipped_upsert(self, dag):
        assert (
            dag.get_task(f"{GROUP}.iteration_finished").trigger_rule
            == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
