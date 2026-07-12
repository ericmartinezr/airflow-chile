INSERT INTO mercadopublico_licitacion_dlq (
    codigo_externo,
    error_message,
    payload,
    fecha_proceso
)
VALUES (
    %(codigo_externo)s,
    %(error_message)s,
    %(payload)s,
    NOW()
)
ON CONFLICT (codigo_externo) DO UPDATE SET
    error_message = EXCLUDED.error_message,
    payload = EXCLUDED.payload,
    fecha_proceso = NOW()