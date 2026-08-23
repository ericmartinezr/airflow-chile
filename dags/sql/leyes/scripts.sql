CREATE TABLE IF NOT EXISTS leyes_chunk_embedding (
    id_proyecto TEXT,
    chunk_index INTEGER,
    titulo TEXT,
    contenido TEXT,
    embeddings vector(3072),
    fecha_creacion TIMESTAMP DEFAULT NOW(),
    fecha_actualizacion TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(id_proyecto, chunk_index)
)

CREATE TABLE IF NOT EXISTS leyes_mocion_chunk_embedding (
    id_proyecto TEXT,
    chunk_index INTEGER,
    contenido TEXT,
    embeddings vector(3072),
    fecha_creacion TIMESTAMP DEFAULT NOW(),
    fecha_actualizacion TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(id_proyecto, chunk_index)
)

CREATE TABLE IF NOT EXISTS leyes_key_insight_embedding (
    id_proyecto TEXT,
    chunk_index INTEGER,
    contenido TEXT,
    embeddings vector(3072),
    fecha_creacion TIMESTAMP DEFAULT NOW(),
    fecha_actualizacion TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(id_proyecto, chunk_index)
)

CREATE TABLE IF NOT EXISTS leyes_error (
    id_proyecto TEXT,
    task TEXT,
    error_message TEXT,
    payload TEXT,
    fecha_proceso TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(id_proyecto, task)
)
