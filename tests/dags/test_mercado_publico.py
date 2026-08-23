"""Tests for the mercado_publico DAG."""
import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowSkipException


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="dags/")
    assert "mercado_publico" in bag.dags, (
        f"mercado_publico DAG not loaded. Import errors: {bag.import_errors}"
    )
    return bag.dags["mercado_publico"]


EXPECTED_TASKS = {
    "get_licitaciones",
    "get_licitacion_detail",
    "save_to_db",
    "save_to_dlq",
    "skip_if_none",
    "skip_if_none_dlq",
    "upsert_licitacion",
    "upsert_licitacion_dlq",
}


class TestDAGStructure:
    def test_dag_loads(self, dag):
        assert dag is not None

    def test_dag_has_expected_tasks(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == EXPECTED_TASKS

    def test_dag_schedule(self, dag):
        assert str(dag.timetable.expression) in ("@daily", "0 0 * * *")

    def test_dag_tags(self, dag):
        assert "scrapping" in dag.tags
        assert "mercadopublico" in dag.tags

    def test_dag_default_args(self, dag):
        assert dag.default_args["retries"] == 1
        assert dag.default_args["owner"] == "emartinez"

    def test_detail_task_uses_pool(self, dag):
        assert dag.get_task("get_licitacion_detail").pool == "mp_licitacion_detail_pool"


class TestTaskFunctions:
    def test_save_to_db_passes_good_record(self, dag):
        fn = dag.get_task("save_to_db").python_callable
        record = {"codigo_externo": "123", "nombre": "Test", "comprador": {}, "adjudicacion": {}, "items": {}, "fechas": {}}
        result = fn(record)
        assert result is not None

    def test_save_to_db_returns_none_for_error_record(self, dag):
        fn = dag.get_task("save_to_db").python_callable
        record = {"codigo_externo": "123", "is_error": True}
        assert fn(record) is None

    def test_save_to_dlq_passes_error_record(self, dag):
        fn = dag.get_task("save_to_dlq").python_callable
        record = {"codigo_externo": "123", "is_error": True, "error_message": "boom"}
        result = fn(record)
        assert result is not None

    def test_save_to_dlq_returns_none_for_good_record(self, dag):
        fn = dag.get_task("save_to_dlq").python_callable
        record = {"codigo_externo": "123", "nombre": "Test"}
        assert fn(record) is None

    def test_skip_if_none_passes_dict(self, dag):
        fn = dag.get_task("skip_if_none").python_callable
        assert fn({"key": "val"}) == {"key": "val"}

    def test_skip_if_none_skips_on_none(self, dag):
        fn = dag.get_task("skip_if_none").python_callable
        with pytest.raises(AirflowSkipException):
            fn(None)

    def test_skip_if_none_dlq_passes_dict(self, dag):
        fn = dag.get_task("skip_if_none_dlq").python_callable
        assert fn({"key": "val"}) == {"key": "val"}

    def test_skip_if_none_dlq_skips_on_none(self, dag):
        fn = dag.get_task("skip_if_none_dlq").python_callable
        with pytest.raises(AirflowSkipException):
            fn(None)
