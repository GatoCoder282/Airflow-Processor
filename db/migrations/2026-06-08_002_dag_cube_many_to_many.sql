-- =====================================================================
-- Migración 002 — Relación muchos-a-muchos DAG → cubos (monitoring.dag_cube)
-- Fecha: 2026-06-08
--
-- Contexto: un DAG puede alimentar varios cubos. Antes se guardaba un único
--   monitoring.dag_catalog.cube_tag (escalar). Esta migración crea la tabla de
--   enlace dag_cube (muchos-a-muchos). El Extractor la repuebla en cada sync de
--   catálogo; los endpoints del Processor agregan los cubos por DAG (string_agg).
--
--   La columna dag_catalog.cube_tag se CONSERVA (cubo primario denormalizado) para
--   no tocar la vista materializada monitoring.dag_current_status.
--
-- Ejecutar como rol con DDL sobre el esquema monitoring. Idempotente (IF NOT EXISTS).
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS monitoring.dag_cube (
    dag_id     varchar(255) NOT NULL,
    region     varchar(10)  NOT NULL DEFAULT 'BO',
    cube_tag   varchar(200) NOT NULL,
    created_at timestamp     NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, region, cube_tag)
);

CREATE INDEX IF NOT EXISTS ix_dag_cube_dag ON monitoring.dag_cube (dag_id, region);

-- FK a dag_catalog (se borra el enlace si se elimina el DAG del catálogo).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_dag_cube_catalog'
    ) THEN
        ALTER TABLE monitoring.dag_cube
            ADD CONSTRAINT fk_dag_cube_catalog
            FOREIGN KEY (dag_id, region)
            REFERENCES monitoring.dag_catalog (dag_id, region) ON DELETE CASCADE;
    END IF;
END $$;

-- Backfill desde el cube_tag escalar actual (un cubo por DAG, si existe).
-- La lista completa se repuebla en el siguiente sync del Extractor.
INSERT INTO monitoring.dag_cube (dag_id, region, cube_tag)
SELECT dag_id, region, cube_tag
FROM monitoring.dag_catalog
WHERE cube_tag IS NOT NULL AND btrim(cube_tag) <> ''
ON CONFLICT DO NOTHING;

COMMIT;
