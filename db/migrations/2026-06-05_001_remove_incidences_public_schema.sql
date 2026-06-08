-- =====================================================================
-- Migración 001 — Eliminar incidencias y dependencias del esquema `public`
-- Fecha: 2026-06-05
--
-- Contexto: se abandona el esquema `public` y se opera solo con `monitoring`.
--   - Las incidencias (report_incidence) y su timeline (incidence_timeline)
--     solo se podían verificar contando reportes en public.report, por lo que
--     quedan obsoletas.
--   - La evaluación de expectativas de reporte (report_run_expectation) tomaba
--     generated_reports_count también de public.report.
--   El backend ya dejó de leer/escribir estos objetos. Este script elimina los
--   objetos físicos que sí es seguro dropear de inmediato.
--
-- Ejecutar conectado a la base del monitoreo, con un rol con permisos de DDL
-- sobre el esquema `monitoring`. Es idempotente (usa IF EXISTS).
-- =====================================================================

BEGIN;

-- 1) Vista materializada del timeline de incidencias (ya no se refresca desde
--    el ViewScheduler). Depende de report_incidence, por eso va primero.
DROP MATERIALIZED VIEW IF EXISTS monitoring.incidence_timeline;

-- 2) Tabla de incidencias a nivel de reporte (ya no se inserta desde el backend).
DROP TABLE IF EXISTS monitoring.report_incidence;

-- 3) (Opcional, salvaguarda del dedup de alertas)
--    Garantiza a nivel de DB una única alerta "abierta" por dedup_key. El backend
--    ya serializa el incremento de occurrence_count con SELECT ... FOR UPDATE;
--    este índice es una protección adicional ante escritores concurrentes.
--    Descomentar si se desea aplicar:
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_open_dedup
--     ON monitoring.alert (region, dedup_key)
--     WHERE resolved = FALSE AND suppressed = FALSE;

COMMIT;

-- =====================================================================
-- FASE 2 (manual — requiere revisar dependencias): report_run_expectation
-- ---------------------------------------------------------------------
-- La tabla monitoring.report_run_expectation puede estar referenciada por la
-- vista monitoring.kpi_summary (p. ej. columnas runs_with_no_reports_today /
-- total_reports_expected). El backend (kpis.py, dynamic_kpis.py) ya dejó de
-- consultarla, pero ANTES de dropearla hay que:
--
--   1. Inspeccionar dependencias:
--        SELECT pg_get_viewdef('monitoring.kpi_summary'::regclass, true);
--
--        SELECT DISTINCT dependent.relname AS vista_dependiente
--          FROM pg_depend d
--          JOIN pg_rewrite   r        ON r.oid = d.objid
--          JOIN pg_class     dependent ON dependent.oid = r.ev_class
--          JOIN pg_class     src      ON src.oid = d.refobjid
--          JOIN pg_namespace ns       ON ns.oid = src.relnamespace
--         WHERE src.relname = 'report_run_expectation'
--           AND ns.nspname  = 'monitoring';
--
--   2. Recrear monitoring.kpi_summary (y cualquier otra vista dependiente) sin
--      las columnas derivadas de report_run_expectation.
--
--   3. Recién entonces dropear la tabla:
--        DROP TABLE IF EXISTS monitoring.report_run_expectation;
--
-- Si se prefiere riesgo cero, dejar la tabla huérfana: el código ya no la usa
-- y no se rompe nada al conservarla.
-- =====================================================================
