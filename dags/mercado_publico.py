"""Pipeline que extrae licitaciones de Mercado Público y las almacena en PostgreSQL."""
import json
import logging
from datetime import timedelta

import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.time_delta import WaitSensor
from airflow.sdk import LoopDecision, TriggerRule, Variable, dag, task, task_group
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from pendulum import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

ENDPOINT_LICITACION = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"

# El endpoint tiene un rate limiter muy estricto: espaciamos las llamadas.
API_CALL_DELAY_SECONDS = 5


def _create_api_session() -> requests.Session:
    """Sesión HTTP con reintentos/backoff para el rate limiter de la API."""
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


# TODO: XCom backend
# https://www.astronomer.io/docs/learn/custom-xcom-backend-strategies
# https://www.astronomer.io/docs/learn/custom-xcom-backend-strategies#use-the-object-storage-xcom-backend


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    doc_md=__doc__,
    default_args={
        "owner": "emartinez",
        "retries": 1,
        "retry_delay": timedelta(seconds=10)
    },
    tags=["scrapping", "mercadopublico"],
)
def mercado_publico():

    @task()
    def get_licitaciones(**context) -> list[str]:
        """
        Extrae el CodigoExterno de las licitaciones publicadas en el día anterior a la ejecución del DAG.
        """
        try:
            start_date = context["logical_date"]
            yesterday = start_date.subtract(days=1)
            record_date = yesterday.format("DDMMYYYY")

            logger.info(f"Logical date: {start_date}")
            logger.info(f"Execution date: {record_date}")

            ticket = Variable.get("API_MP", None)
            if not ticket:
                raise ValueError("¡Debes asignar el ticket!")

            params = {
                "fecha": record_date,
                "ticket": ticket
            }

            r = requests.get(ENDPOINT_LICITACION, params=params)
            r.raise_for_status()
            licitaciones = r.json()["Listado"]

            # Retorna solo el codigo externo
            # El detalle es en una tarea siguiente
            if not licitaciones:
                raise ValueError(
                    f"No existen licitaciones para la fecha {record_date}")

            lista = [x["CodigoExterno"] for x in licitaciones]

            # TODO: Para probar por el momento
            return lista

        except Exception as e:
            logger.error("Error extrayendo licitaciones")
            logger.error(e, exc_info=True)
            raise AirflowFailException

    @task_group()
    def get_licitacion_detail_loop(licitaciones):
        """
        Task Group que contiene el loop de licitaciones a procesar.
        Cada iteración del loop obtiene el detalle de una licitación y lo procesa.
        """
        # Las tareas mapeadas deben crearse dentro del grupo: si se definen fuera,
        # quedan en el DAG raíz y el loop detecta más de una tarea terminal.
        upsert_licitacion = SQLExecuteQueryOperator.partial(
            task_id="upsert_licitacion",
            conn_id="airflow_cl_db",
            sql="sql/mercadopublico/upsert_licitacion.sql",
            autocommit=True,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        upsert_licitacion_dlq = SQLExecuteQueryOperator.partial(
            task_id="upsert_licitacion_dlq",
            conn_id="airflow_cl_db",
            sql="sql/mercadopublico/upsert_licitacion_dlq.sql",
            autocommit=True,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        @task()
        def get_licitacion_to_process(licitaciones, *, loop):
            """
            Obtiene la licitación a procesar en la iteración actual del loop.
            También retorna la lista de licitaciones restantes para procesar en las siguientes iteraciones.
            """
            print(f"Loop index: {loop.index}, Loop: {loop}")

            list_licitaciones = []
            if loop.index == 1:
                list_licitaciones = licitaciones
            else:
                list_licitaciones = loop.previous.get(
                    "next_licitaciones") or []

            licitacion = None
            if loop.index == 1 and list_licitaciones or loop.index > 1 and list_licitaciones:
                licitacion = list_licitaciones[0]

            print(
                f"Processing licitacion {loop.index} of {len(list_licitaciones)} licitaciones")

            return {
                "current_licitacion": licitacion,
                "next_licitaciones": list_licitaciones[1:] if licitacion else licitaciones
            }

        @task(
            max_active_tis_per_dag=1,
            retries=5,
            retry_delay=timedelta(seconds=10),
            pool="mp_licitacion_detail_pool"
        )
        def process_licitacion(data: dict) -> dict:
            """
            Procesa la licitación obtenida en la iteración actual del loop.
            """
            if not data:
                logger.error("No licitacion to process")
                raise AirflowSkipException

            codigo = data.get("current_licitacion")

            logger.info(f"Codigo procesado: {codigo}")

            ticket = Variable.get("API_MP", None)
            params = {
                "codigo": codigo,
                "ticket": ticket
            }
            r = None
            try:
                r = _create_api_session().get(
                    ENDPOINT_LICITACION, params=params, timeout=30
                )
                r.raise_for_status()

                licitacion_data = r.json()

                # Error al consultar, conexiones simulteaneas?s
                if "Codigo" in licitacion_data and "Mensaje" in licitacion_data:
                    raise ValueError(
                        f"Error al consultar: \nCodigo: {licitacion_data.get('Codigo')}\n{licitacion_data.get('Mensaje')}"
                    )

                listado = licitacion_data.get("Listado")
                if not listado:
                    raise ValueError(
                        f"No existe licitacion para el codigo {codigo}")

                licitacion = listado[0]

                return {
                    "codigo_externo": licitacion.get("CodigoExterno"),
                    "nombre": licitacion.get("Nombre"),
                    "codigo_estado": licitacion.get("CodigoEstado"),
                    "descripcion": licitacion.get("Descripcion"),
                    "fecha_cierre": licitacion.get("FechaCierre"),
                    "estado": licitacion.get("Estado"),
                    "comprador": licitacion.get("Comprador") or {},
                    "dias_cierre_licitacion": licitacion.get("DiasCierreLicitacion"),
                    "informada": licitacion.get("Informada"),
                    "codigo_tipo": licitacion.get("CodigoTipo"),
                    "tipo": licitacion.get("Tipo"),
                    "tipo_convocatoria": licitacion.get("TipoConvocatoria"),
                    "moneda": licitacion.get("Moneda"),
                    "etapas": licitacion.get("Etapas"),
                    "estado_etapas": licitacion.get("EstadoEtapas"),
                    "toma_razon": licitacion.get("TomaRazon"),
                    "estado_publicidad_ofertas": licitacion.get("EstadoPublicidadOfertas"),
                    "justificacion_publicidad": licitacion.get("JustificacionPublicidad"),
                    "contrato": licitacion.get("Contrato"),
                    "obras": licitacion.get("Obras"),
                    "cantidad_reclamos": licitacion.get("CantidadReclamos"),
                    "fechas": licitacion.get("Fechas", {}) or {},
                    "unidad_tiempo_evaluacion": licitacion.get("UnidadTiempoEvaluacion"),
                    "direccion_visita": licitacion.get("DireccionVisita"),
                    "direccion_entrega": licitacion.get("DireccionEntrega"),
                    "estimacion": licitacion.get("Estimacion"),
                    "fuente_financiamiento": licitacion.get("FuenteFinanciamiento"),
                    "visibilidad_monto": licitacion.get("VisibilidadMonto"),
                    "monto_estimado": licitacion.get("MontoEstimado"),
                    "tiempo": licitacion.get("Tiempo"),
                    "unidad_tiempo": licitacion.get("UnidadTiempo"),
                    "modalidad": licitacion.get("Modalidad"),
                    "tipo_pago": licitacion.get("TipoPago"),
                    "nombre_responsable_pago": licitacion.get("NombreResponsablePago"),
                    "email_responsable_pago": licitacion.get("EmailResponsablePago"),
                    "nombre_responsable_contrato": licitacion.get("NombreResponsableContrato"),
                    "email_responsable_contrato": licitacion.get("EmailResponsableContrato"),
                    "fono_responsable_contrato": licitacion.get("FonoResponsableContrato"),
                    "prohibicion_contratacion": licitacion.get("ProhibicionContratacion"),
                    "sub_contratacion": licitacion.get("SubContratacion"),
                    "unidad_tiempo_duracion_contrato": licitacion.get("UnidadTiempoDuracionContrato"),
                    "tiempo_duracion_contrato": licitacion.get("TiempoDuracionContrato"),
                    "tipo_duracion_contrato": licitacion.get("TipoDuracionContrato"),
                    "justificacion_monto_estimado": licitacion.get("JustificacionMontoEstimado"),
                    "observacion_contract": licitacion.get("ObservacionContract"),
                    "extension_plazo": licitacion.get("ExtensionPlazo"),
                    "es_base_tipo": licitacion.get("EsBaseTipo"),
                    "unidad_tiempo_contrato_licitacion": licitacion.get("UnidadTiempoContratoLicitacion"),
                    "valor_tiempo_renovacion": licitacion.get("ValorTiempoRenovacion"),
                    "periodo_tiempo_renovacion": licitacion.get("PeriodoTiempoRenovacion"),
                    "es_renovable": licitacion.get("EsRenovable"),
                    "codigo_bip": licitacion.get("CodigoBIP"),
                    "adjudicacion": licitacion.get("Adjudicacion", {}) or {},
                    "items": licitacion.get("Items", {}) or {},
                }
            except Exception as e:
                logger.error(
                    f"Error extrayendo el detalle de la licitacion {codigo}")
                logger.exception("Error processing licitacion")
                return {
                    "codigo_externo": codigo,
                    "error_message": str(e),
                    "payload": r.text if r is not None else "",
                    "is_error": True,
                }

        @task()
        def prepare_licitacion(result: dict) -> dict:
            """
            Prepara el registro válido para el upsert. Serializa los campos JSONB:
            psycopg no puede adaptar un ``dict`` al placeholder ``%s``.
            Los registros con error se omiten para que este SQL no se ejecute.
            """
            if result.get("is_error"):
                raise AirflowSkipException

            prepared = dict(result)
            for field in ("comprador", "adjudicacion", "items", "fechas"):
                prepared[field] = json.dumps(
                    prepared.get(field) or {}, ensure_ascii=False
                )
            return prepared

        @task()
        def prepare_licitacion_dlq(result: dict) -> dict:
            """
            Prepara el registro con error para el upsert de la DLQ, garantizando
            las tres claves que exige el SQL. Los registros válidos se omiten.
            """
            if not result.get("is_error"):
                raise AirflowSkipException

            return {
                "codigo_externo": result.get("codigo_externo"),
                "error_message": str(result.get("error_message") or ""),
                "payload": str(result.get("payload") or ""),
            }

        @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        def iteration_finished(data: dict) -> dict:
            """
            Única tarea terminal del loop: combina las dos ramas y devuelve el
            resultado de la iteración, que Airflow expone como ``loop.result``.
            """
            logger.info("Iteration finished")
            return {"next_licitaciones": data.get("next_licitaciones") or []}

        # Sensores de tiempo relativos al inicio de la tarea: a diferencia de
        # TimeDeltaSensor, espera desde que arranca y no desde el logical date.
        # Con deferrable=True libera el worker (Astro Runtime incluye triggerer).
        wait_before_process = WaitSensor(
            task_id="wait_before_process",
            time_to_wait=timedelta(seconds=API_CALL_DELAY_SECONDS),
            deferrable=True,
        )

        _get_licitacion_to_process = get_licitacion_to_process(licitaciones)
        _process_licitacion = process_licitacion(_get_licitacion_to_process)

        # El XComArg ya hace que process dependa de get; el sensor se interpone
        # explícitamente para espaciar la llamada a la API.
        _get_licitacion_to_process >> wait_before_process >> _process_licitacion
        _prepared_licitacion = prepare_licitacion(_process_licitacion)
        _prepared_licitacion_dlq = prepare_licitacion_dlq(_process_licitacion)

        # Cada rama se decide con un skip (AirflowSkipException): una expansión
        # vacía omite el SQL correspondiente, incluso dentro de un loop donde
        # el skip de un BranchPythonOperator no llega a las tareas mapeadas.
        _upsert_licitacion = upsert_licitacion.expand(
            parameters=[_prepared_licitacion]
        )
        _upsert_licitacion_dlq = upsert_licitacion_dlq.expand(
            parameters=[_prepared_licitacion_dlq]
        )

        [
            wait_before_process,
            _upsert_licitacion,
            _upsert_licitacion_dlq,
        ] >> iteration_finished(_get_licitacion_to_process)

    def evaluate_loop(*, loop):
        licitaciones = loop.result.get("next_licitaciones") or []
        if not licitaciones:
            return LoopDecision(stop=True, reason="All licitaciones processed")

        return LoopDecision(stop=False, reason="Continue processing licitaciones")

    @task
    def finished():
        """Tarea final: todas las licitaciones del día fueron procesadas."""
        logger.info("All licitaciones processed")

    _get_licitaciones = get_licitaciones()

    process_licitaciones = (
        get_licitacion_detail_loop
        .override(group_id="process_licitaciones")
        .partial(licitaciones=_get_licitaciones)
        .loop(
            max_iterations=100_000,
            until=evaluate_loop
        )
    )

    process_licitaciones >> finished()


mercado_publico()
