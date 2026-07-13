"""Pipeline que extrae licitaciones de Mercado Público y las almacena en PostgreSQL."""
from airflow.sdk import Variable, TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
from datetime import timedelta
import json
import requests
import logging

logger = logging.getLogger(__name__)

ENDPOINT_LICITACION = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"

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
        "retry_Delay": timedelta(seconds=10)
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

            lista = list(map(lambda x: x["CodigoExterno"], licitaciones))

            # TODO: Para probar por el momento
            return lista  # [:5]

        except Exception as e:
            logger.error("Error extrayendo licitaciones")
            logger.error(e, exc_info=True)
            raise AirflowFailException

    @task(
        max_active_tis_per_dag=1,
        retries=5,
        retry_delay=timedelta(seconds=10),
        pool="mp_licitacion_detail_pool"
    )
    def get_licitacion_detail(codigo: str) -> dict:
        logger.info(f"Codigo procesado: {codigo}")

        ticket = Variable.get("API_MP", None)
        params = {
            "codigo": codigo,
            "ticket": ticket
        }
        r = requests.get(ENDPOINT_LICITACION, params=params)
        try:
            r.raise_for_status()
            data = r.json()

            # Error al consultar, conexiones simulteaneas?s
            if "Codigo" in data and "Mensaje" in data:
                raise ValueError(
                    f"Error al consultar: \nCodigo: {data['Codigo']}\n{data['Mensaje']}"
                )

            licitacion = data["Listado"][0]
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
            logger.error(e, exc_info=True)
            return {
                "codigo_externo": codigo,
                "error_message": str(e),
                "payload": r.text,
                "is_error": True,
            }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def save_to_db(result: dict):
        if "is_error" in result and result["is_error"]:
            return None

        result["comprador"] = json.dumps(
            result["comprador"],
            ensure_ascii=False
        )
        result["adjudicacion"] = json.dumps(
            result["adjudicacion"],
            ensure_ascii=False
        )
        result["items"] = json.dumps(result["items"], ensure_ascii=False)
        result["fechas"] = json.dumps(result["fechas"], ensure_ascii=False)
        return result

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def save_to_dlq(result: dict):
        if "is_error" not in result or not result["is_error"]:
            return None

        return {
            "codigo_externo": result.get("codigo_externo"),
            "error_message": result.get("error_message"),
            "payload": result.get("payload"),
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def skip_if_none(x):
        if x is None:
            raise AirflowSkipException
        return x

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def skip_if_none_dlq(x):
        if x is None:
            raise AirflowSkipException
        return x

    upsert_licitacion = SQLExecuteQueryOperator.partial(
        task_id="upsert_licitacion",
        conn_id="airflow_cl_db",
        sql="sql/mercadopublico/upsert_licitacion.sql",
        autocommit=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    upsert_licitacion_dlq = SQLExecuteQueryOperator.partial(
        task_id="upsert_licitacion_dlq",
        conn_id="airflow_cl_db",
        sql="sql/mercadopublico/upsert_licitacion_dlq.sql",
        autocommit=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    _get_licitaciones = get_licitaciones()
    _get_licitacion_detail = get_licitacion_detail.expand(
        codigo=_get_licitaciones
    )

    _save_to_db = save_to_db.expand(result=_get_licitacion_detail)
    _save_to_dlq = save_to_dlq.expand(result=_get_licitacion_detail)

    _prepared_db = skip_if_none.expand(x=_save_to_db)
    _prepared_dlq = skip_if_none_dlq.expand(x=_save_to_dlq)

    upsert_licitacion.expand(parameters=_prepared_db)
    upsert_licitacion_dlq.expand(parameters=_prepared_dlq)


mercado_publico()
