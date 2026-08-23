INSERT INTO leyes_error (
    id_proyecto,
    task,
    error_message,
    payload,
    fecha_proceso
)
VALUES (
    %(id_proyecto)s,
    %(task)s,
    %(error_message)s,
    %(payload)s,
    NOW()
)
ON CONFLICT (id_proyecto, task) DO UPDATE SET
    error_message = EXCLUDED.error_message,
    payload = EXCLUDED.payload,
    fecha_proceso = NOW()
