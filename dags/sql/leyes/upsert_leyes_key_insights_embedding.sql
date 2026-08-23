INSERT INTO leyes_key_insight_embedding (
    id_proyecto,
    chunk_index,
    contenido,
    embeddings,
    fecha_creacion,
    fecha_actualizacion
)
SELECT
    c->>'id_proyecto',
    (c->>'chunk_index')::INTEGER,
    c->>'contenido',
    (c->>'embeddings')::vector,
    NOW(),
    NOW()
FROM jsonb_array_elements(%(chunks)s::jsonb) AS c
ON CONFLICT (id_proyecto, chunk_index) DO UPDATE SET
    contenido = EXCLUDED.contenido,
    embeddings = EXCLUDED.embeddings,
    fecha_actualizacion = NOW()
