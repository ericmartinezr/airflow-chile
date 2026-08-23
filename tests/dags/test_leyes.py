"""Tests for the leyes DAG."""
import json
import sys
import unittest.mock as mock
from types import SimpleNamespace

import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowSkipException

from include.leyes.dlq import dlq_failure_callback
from include.leyes.download import download_mocion
from include.leyes.embed import CHUNK_OVERLAP, CHUNK_SIZE, NoVectorsError, embed_pdf, embed_text
from include.leyes.extract import _normalizar_ley

EXPECTED_TASKS = {
    "extract_leyes_publicadas",
    "mocion.download_mocion",
    "mocion.embed_mocion",
    "mocion.upsert_leyes_mocion_embedding",
    "ley.extract_ley",
    "ley.extract_ley_key_insights",
    "ley.embed_key_insight",
    "ley.upsert_leyes_key_insights_embedding",
    "ley.embed_ley",
    "ley.upsert_leyes_embedding",
}


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="dags/")
    assert "ley_chile" in bag.dags, (
        f"ley_chile DAG not loaded. Import errors: {bag.import_errors}"
    )
    return bag.dags["ley_chile"]


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
        assert "leyes" in dag.tags

    def test_dag_default_args(self, dag):
        assert dag.default_args["retries"] == 1
        assert dag.default_args["owner"] == "emartinez"
        assert dag.default_args["retry_exponential_backoff"] is True
        assert dag.default_args["max_active_tis_per_dag"] == 2

    def test_dag_timezone(self, dag):
        assert str(dag.timezone) == "America/Santiago"

    def test_dag_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_etl_tasks_have_timeout_and_callback(self, dag):
        etl_tasks = [
            "mocion.download_mocion",
            "mocion.embed_mocion",
            "ley.extract_ley",
            "ley.extract_ley_key_insights",
            "ley.embed_ley",
        ]
        for task_id in etl_tasks:
            task = dag.get_task(task_id)
            assert task.execution_timeout is not None, task_id
            assert task.on_failure_callback is not None, task_id

    def test_upsert_tasks_use_all_done_trigger(self, dag):
        for task_id in (
            "mocion.upsert_leyes_mocion_embedding",
            "ley.upsert_leyes_key_insights_embedding",
            "ley.upsert_leyes_embedding",
        ):
            from airflow.sdk import TriggerRule
            assert dag.get_task(task_id).trigger_rule == TriggerRule.ALL_DONE


class TestSqlQueries:
    def _sql(self, nombre):
        return (open(f"dags/sql/leyes/{nombre}").read())

    def test_upsert_key_insights_embedding_pattern(self):
        sql = self._sql("upsert_leyes_key_insights_embedding.sql")
        assert "INSERT INTO leyes_key_insight_embedding" in sql
        assert "jsonb_array_elements(%(chunks)s::jsonb)" in sql
        assert "(c->>'embeddings')::vector" in sql
        assert "ON CONFLICT (id_proyecto, chunk_index) DO UPDATE SET" in sql
        assert "fecha_actualizacion = NOW()" in sql

    def test_scripts_ddl_cubre_todas_las_tablas(self):
        ddl = self._sql("scripts.sql")
        for tabla in (
            "leyes_chunk_embedding",
            "leyes_mocion_chunk_embedding",
            "leyes_key_insight_embedding",
            "leyes_error",
        ):
            assert f"CREATE TABLE IF NOT EXISTS {tabla}" in ddl

    def test_upsert_error_reused_by_callback(self):
        from include.leyes.dlq import SQL_DLQ

        assert SQL_DLQ.is_file()
        assert "INSERT INTO leyes_error" in SQL_DLQ.read_text()


class TestNormalizacion:
    def test_normaliza_claves_y_descarta_campos(self):
        raw = {
            "ID_PROYECTO": "123-2024",
            "TITULO": '"Ley de prueba"',
            "LINK_BCN": "https://www.bcn.cl/leyes",
            "LINK_DOCUMENTO_MENSAJE_MOCION": "https://senado.cl/mocion.pdf",
            "ID_NORMA": "9999",
        }
        assert _normalizar_ley(raw) == {
            "id_proyecto": "123-2024",
            "titulo": "Ley de prueba",
            "link_bcn": "https://www.bcn.cl/leyes",
            "link_mocion": "https://senado.cl/mocion.pdf",
        }

    def test_titulo_sin_comillas_queda_intacto(self):
        raw = {"ID_PROYECTO": "1", "TITULO": 'Titulo con "comillas" internas'}
        assert _normalizar_ley(raw)["titulo"] == 'Titulo con "comillas" internas'

    def test_campos_ausentes_no_rompen(self):
        norma = _normalizar_ley({"ID_PROYECTO": "1"})
        assert norma["titulo"] == ""
        assert norma["link_bcn"] is None


class TestTaskFunctions:
    def test_extract_ley_returns_dict_with_text(self, dag):
        fn = dag.get_task("ley.extract_ley").python_callable
        data = {
            "id_proyecto": "123",
            "titulo": "Ley Test",
            "link_bcn": "http://example.com",
        }
        mod = sys.modules[fn.__module__]
        with mock.patch.object(mod, "_extract_ley", return_value="Texto de la ley"):
            result = fn(data)
        assert result["text"] == "Texto de la ley"
        assert result["id_proyecto"] == "123"

    def test_extract_ley_propagates_failure(self, dag):
        """Los errores ya no se capturan como dict: fallan y van a la DLQ vía callback."""
        fn = dag.get_task("ley.extract_ley").python_callable
        mod = sys.modules[fn.__module__]
        data = {"id_proyecto": "456", "link_bcn": "http://bad.com"}
        with mock.patch.object(mod, "_extract_ley", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                fn(data)

    def test_key_insights_builds_params(self, dag):
        fn = dag.get_task("ley.extract_ley_key_insights").python_callable
        mod = sys.modules[fn.__module__]
        data = {"id_proyecto": "123", "titulo": "T", "text": "contenido"}
        with mock.patch.object(
            mod, "_get_extract_ley_key_insights", return_value="- punto clave"
        ):
            assert fn(data) == {
                "id_proyecto": "123",
                "titulo": "T",
                "key_insights": "- punto clave",
            }

    @pytest.mark.parametrize("campo,faltante", [("text", "texto"), ("titulo", "título")])
    def test_key_insights_requires_inputs(self, dag, campo, faltante):
        fn = dag.get_task("ley.extract_ley_key_insights").python_callable
        data = {"id_proyecto": "123", "text": "x", "titulo": "y"}
        del data[campo]
        with pytest.raises(ValueError, match=faltante):
            fn(data)

    def test_embed_ley_passes_params_through(self, dag):
        fn = dag.get_task("ley.embed_ley").python_callable
        mod = sys.modules[fn.__module__]
        esperado = {"chunks": json.dumps([{"chunk_index": 0}])}
        with mock.patch.object(mod, "_get_embed_text", return_value=esperado):
            assert fn({"id_proyecto": "1"}) == esperado

    def test_embed_ley_skips_without_vectors(self, dag):
        fn = dag.get_task("ley.embed_ley").python_callable
        mod = sys.modules[fn.__module__]
        with mock.patch.object(
            mod,
            "_get_embed_text",
            side_effect=NoVectorsError("sin contenido"),
        ):
            with pytest.raises(AirflowSkipException):
                fn({"id_proyecto": "1"})

    def test_embed_mocion_skips_without_vectors(self, dag):
        fn = dag.get_task("mocion.embed_mocion").python_callable
        mod = sys.modules[fn.__module__]
        with mock.patch.object(
            mod,
            "_get_embed_pdf",
            side_effect=NoVectorsError("sin contenido"),
        ):
            with pytest.raises(AirflowSkipException):
                fn({"id_proyecto": "1"})


class TestEmbeddingChunking:
    def test_markdown_splitter_chunks_are_content_not_chars(self):
        from langchain_text_splitters import MarkdownTextSplitter

        md = "## Sección\n\nLorem ipsum dolor sit amet consectetur.\n" * 60
        docs = MarkdownTextSplitter(
            chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
        ).create_documents([md])

        assert len(docs) > 1
        assert all(len(doc.page_content) > 10 for doc in docs)


def _embeddings_falsos(contents):
    return SimpleNamespace(
        embeddings=[
            SimpleNamespace(values=[float(i)]) for i, _ in enumerate(contents)
        ]
    )


class TestEmbedParams:
    """embed_text / embed_pdf retornan directamente los parámetros del upsert."""

    _DATA_LEY = {"id_proyecto": "123", "titulo": "T", "text": "palabra " * 800}

    def test_embed_text_retorna_parametros_json(self):
        with mock.patch(
            "airflow.sdk.Variable.get", return_value="test-key"
        ), mock.patch("google.genai.Client") as client_cls:
            client = client_cls.return_value.__enter__.return_value
            client.models.embed_content.side_effect = (
                lambda model, contents: _embeddings_falsos(contents)
            )
            resultado = embed_text(self._DATA_LEY)

        assert set(resultado) == {"chunks"}
        filas = json.loads(resultado["chunks"])
        assert len(filas) >= 2
        assert filas[0]["id_proyecto"] == "123"
        assert filas[0]["titulo"] == "T"
        for i, fila in enumerate(filas):
            assert fila["chunk_index"] == i
            assert set(fila) == {
                "id_proyecto",
                "chunk_index",
                "titulo",
                "contenido",
                "embeddings",
            }
            assert isinstance(fila["embeddings"], list)

    def test_embed_pdf_retorna_parametros_json(self):
        data = {
            "id_proyecto": "456",
            "titulo": "Moción",
            "path": "/tmp/mocion_456.pdf",
        }
        with mock.patch(
            "airflow.sdk.Variable.get", return_value="test-key"
        ), mock.patch(
            "pymupdf4llm.to_markdown", return_value="## Encabezado\n\n" + "texto " * 500
        ), mock.patch("google.genai.Client") as client_cls:
            client = client_cls.return_value.__enter__.return_value
            client.models.embed_content.side_effect = (
                lambda model, contents: _embeddings_falsos(contents)
            )
            resultado = embed_pdf(data)

        filas = json.loads(resultado["chunks"])
        assert len(filas) >= 1
        assert all(fila["id_proyecto"] == "456" for fila in filas)


class TestDescargaMocion:
    def test_download_propaga_titulo_y_ruta(self, tmp_path):
        """Regresión P2: el registro completo (con titulo) debe llegar al embed."""
        class RespuestaFalsa:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size=8192):
                yield b"%PDF-falso"

        data = {
            "id_proyecto": "123",
            "titulo": "Ley Test",
            "link_mocion": "http://senado.cl/mocion.pdf",
        }
        destino = tmp_path / "descargas"
        destino.mkdir()
        with mock.patch(
            "include.leyes.download.requests.get", return_value=RespuestaFalsa()
        ) as get_falso:
            resultado = download_mocion(data, download_dir=str(destino))

        get_falso.assert_called_once_with(
            "http://senado.cl/mocion.pdf", stream=True, timeout=(10, 60)
        )
        assert resultado["id_proyecto"] == "123"
        assert resultado["titulo"] == "Ley Test"
        assert resultado["path"].endswith("mocion_123.pdf")
        assert (destino / "mocion_123.pdf").read_bytes() == b"%PDF-falso"


class TestDlqCallback:
    REGISTRO = {"id_proyecto": "789", "link_bcn": "http://bcn.cl/x"}

    @staticmethod
    def _ti(map_index=0, registro=REGISTRO):
        def xcom_pull(**kwargs):
            if "map_indexes" in kwargs:
                # upstream mapeado: solo la fila del índice actual
                return registro if kwargs["map_indexes"] == map_index else None
            # upstream sin mapear (map_index = -1 en la tabla xcom)
            return registro

        return SimpleNamespace(
            task_id="ley.extract_ley", map_index=map_index, xcom_pull=xcom_pull
        )

    def _contexto(self, ti):
        return {"task_instance": ti, "exception": RuntimeError("boom")}

    def test_registra_error_en_dlq(self):
        with mock.patch("include.leyes.dlq.PostgresHook") as hook_cls:
            hook = hook_cls.return_value
            dlq_failure_callback(self._contexto(self._ti()))

        hook_cls.assert_called_once_with(postgres_conn_id="airflow_cl_db")
        hook.run.assert_called_once()
        sql, = hook.run.call_args.args
        parametros = hook.run.call_args.kwargs["parameters"]
        assert "INSERT INTO leyes_error" in sql
        assert hook.run.call_args.kwargs["autocommit"] is True
        assert parametros["id_proyecto"] == "789"
        assert parametros["task"] == "ley.extract_ley"
        assert "boom" in parametros["error_message"]
        assert parametros["payload"] == "http://bcn.cl/x"

    def test_no_lanza_si_falla_el_hook(self):
        with mock.patch("include.leyes.dlq.PostgresHook") as hook_cls:
            hook_cls.return_value.run.side_effect = RuntimeError("db caída")
            dlq_failure_callback(self._contexto(self._ti()))

    def test_task_fuera_del_mapa_se_ignora_sin_lanzar(self):
        ti = SimpleNamespace(
            task_id="otro.task",
            map_index=-1,
            xcom_pull=lambda **kwargs: {"id_proyecto": "1"},
        )
        with mock.patch("include.leyes.dlq.PostgresHook") as hook_cls:
            dlq_failure_callback(self._contexto(ti))
        hook_cls.return_value.run.assert_not_called()
