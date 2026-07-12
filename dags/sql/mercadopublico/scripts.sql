
CREATE TABLE IF NOT EXISTS mercadopublico_parametros (
    codigo TEXT,
    tipo TEXT,
    descripcion TEXT,
    PRIMARY KEY(codigo, tipo)
)

CREATE TABLE IF NOT EXISTS mercadopublico_licitacion (
    codigo_externo TEXT PRIMARY KEY,
    nombre TEXT,
    codigo_estado INTEGER,
    descripcion TEXT,
    fecha_cierre TIMESTAMP,
    estado TEXT,
    comprador JSONB,
    dias_cierre_licitacion TEXT,
    informada INTEGER,
    codigo_tipo INTEGER,
    tipo TEXT,
    tipo_convocatoria TEXT,
    moneda TEXT,
    etapas INTEGER,
    estado_etapas TEXT,
    toma_razon TEXT,
    estado_publicidad_ofertas INTEGER,
    justificacion_publicidad TEXT,
    contrato TEXT,
    obras TEXT,
    cantidad_reclamos INTEGER,
    fechas JSONB,
    unidad_tiempo_evaluacion INTEGER,
    direccion_visita TEXT,
    direccion_entrega TEXT,
    estimacion INTEGER,
    fuente_financiamiento TEXT,
    visibilidad_monto INTEGER,
    monto_estimado NUMERIC,
    tiempo TEXT,
    unidad_tiempo TEXT,
    modalidad INTEGER,
    tipo_pago TEXT,
    nombre_responsable_pago TEXT,
    email_responsable_pago TEXT,
    nombre_responsable_contrato TEXT,
    email_responsable_contrato TEXT,
    fono_responsable_contrato TEXT,
    prohibicion_contratacion TEXT,
    sub_contratacion TEXT,
    unidad_tiempo_duracion_contrato INTEGER,
    tiempo_duracion_contrato TEXT,
    tipo_duracion_contrato TEXT,
    justificacion_monto_estimado TEXT,
    observacion_contract TEXT,
    extension_plazo INTEGER,
    es_base_tipo INTEGER,
    unidad_tiempo_contrato_licitacion TEXT,
    valor_tiempo_renovacion TEXT,
    periodo_tiempo_renovacion TEXT,
    es_renovable INTEGER,
    codigo_bip TEXT,
    adjudicacion JSONB,
    items JSONB,
    fecha_proceso TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mercadopublico_licitacion_dlq (
    codigo_externo TEXT PRIMARY KEY,
    error_message TEXT,
    payload TEXT,
    fecha_proceso TIMESTAMP DEFAULT NOW()
);


INSERT INTO mercadopublico_parametros (codigo, tipo, descripcion) VALUES

('1', 'CODIGO_TIPO_LICITACION', 'Pública'),
('2', 'CODIGO_TIPO_LICITACION', 'Privada'),
('1', 'TIPO_CONVOCATORIA', 'Abierto'),
('0', 'TIPO_CONVOCATORIA', 'Cerrada'),

-- 3.1 Tipo de Licitación
('L1', 'TIPO_LICITACION', 'Licitación Pública Menor a 100 UTM'),
('LE', 'TIPO_LICITACION', 'Licitación Pública igual o superior a 100 UTM e inferior a 1.000 UTM'),
('LP', 'TIPO_LICITACION', 'Licitación Pública igual o superior a 1.000 UTM e inferior a 2.000 UTM'),
('LQ', 'TIPO_LICITACION', 'Licitación Pública igual o superior a 2.000 UTM e inferior a 5.000 UTM'),
('LR', 'TIPO_LICITACION', 'Licitación Pública igual o superior a 5.000 UTM'),
('E2', 'TIPO_LICITACION', 'Licitación Privada Menor a 100 UTM'),
('CO', 'TIPO_LICITACION', 'Licitación Privada igual o superior a 100 UTM e inferior a 1000 UTM'),
('B2', 'TIPO_LICITACION', 'Licitación Privada igual o superior a 1000 UTM e inferior a 2000 UTM'),
('H2', 'TIPO_LICITACION', 'Licitación Privada igual o superior a 2000 UTM e inferior a 5000 UTM'),
('I2', 'TIPO_LICITACION', 'Licitación Privada Mayor a 5000 UTM'),
('LS', 'TIPO_LICITACION', 'Licitación Pública Servicios personales especializados'),

-- 3.2 Unidad Monetaria
('CLP', 'UNIDAD_MONETARIA', 'Peso Chileno'),
('CLF', 'UNIDAD_MONETARIA', 'Unidad de Fomento'),
('USD', 'UNIDAD_MONETARIA', 'Dólar Americano'),
('UTM', 'UNIDAD_MONETARIA', 'Unidad Tributaria Mensual'),
('EUR', 'UNIDAD_MONETARIA', 'Euro'),

-- 3.3 Monto Estimado
('1', 'CODIGO_MONTO_ESTIMADO', 'Presupuesto Disponible'),
('2', 'CODIGO_MONTO_ESTIMADO', 'Precio Referencial'),
('3', 'CODIGO_MONTO_ESTIMADO', 'Monto no es posible de estimar'),

-- 3.4 Modalidad de Pago
('1', 'MODALIDAD_PAGO', 'Pago a 30 días'),
('2', 'MODALIDAD_PAGO', 'Pago a 30, 60 y 90 días'),
('3', 'MODALIDAD_PAGO', 'Pago al día'),
('4', 'MODALIDAD_PAGO', 'Pago Anual'),
('5', 'MODALIDAD_PAGO', 'Pago Bimensual'),
('6', 'MODALIDAD_PAGO', 'Pago Contra Entrega Conforme'),
('7', 'MODALIDAD_PAGO', 'Pagos Mensuales'),
('8', 'MODALIDAD_PAGO', 'Pago Por Estado de Avance'),
('9', 'MODALIDAD_PAGO', 'Pago Trimestral'),
('10', 'MODALIDAD_PAGO', 'Pago a 60 días'),

-- 3.5 Unidad de Tiempo
('1', 'UNIDAD_TIEMPO', 'Horas'),
('2', 'UNIDAD_TIEMPO', 'Días'),
('3', 'UNIDAD_TIEMPO', 'Semanas'),
('4', 'UNIDAD_TIEMPO', 'Meses'),
('5', 'UNIDAD_TIEMPO', 'Años'),

-- 3.7 Tipo Acto Administrativo
('1', 'TIPO_ACTO_ADMINISTRATIVO', 'Autorización'),
('2', 'TIPO_ACTO_ADMINISTRATIVO', 'Resolución'),
('3', 'TIPO_ACTO_ADMINISTRATIVO', 'Acuerdo'),
('4', 'TIPO_ACTO_ADMINISTRATIVO', 'Decreto'),
('5', 'TIPO_ACTO_ADMINISTRATIVO', 'Otros'),

-- Código estado
('5', 'ESTADO', 'Publicada'),
('6', 'ESTADO', 'Cerrada'),
('7', 'ESTADO', 'Desierta'),
('8', 'ESTADO', 'Adjudicada'),
('18', 'ESTADO', 'Revocada'),
('19', 'ESTADO', 'Suspendida');
