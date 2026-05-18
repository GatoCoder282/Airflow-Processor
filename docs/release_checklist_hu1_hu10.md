# Release Checklist HU1-HU10

## Estado general

- [x] Sprint 1 completado
- [x] Sprint 2 completado
- [x] Sprint 3 completado
- [x] Sprint 4 completado
- [x] Sprint 5 (observabilidad + testing/cierre) completado

## HU1 - Visualizacion de fallos generales

- [x] KPI diario de fallos
- [x] KPI semanal de fallos
- [x] Endpoint estable para consumo dashboard: /kpis y /kpis/extended

## HU2 - Monitoreo de reportes

- [x] Persistencia de expectation por run (report_run_expectation)
- [x] Deteccion de reportes faltantes
- [x] Consulta por incidencias priorizadas

## HU3 - Identificacion de URLs fallidas

- [x] Endpoint de URLs priorizadas: /urls/prioritized
- [x] Export Excel: /urls/prioritized/export.xlsx
- [x] Orden por prioridad y frecuencia de fallo

## HU4 - Priorizacion de incidencias

- [x] priority_score calculado con pesos
- [x] Jerarquia fija reporte > fuente > frecuencia
- [x] Flujo de estado: open, in_progress, resolved, suppressed

## HU5 - Alertas de retrasos

- [x] Evaluacion por SLA / download_delay
- [x] Alertas con deduplicacion para evitar tormenta

## HU6 - Analisis temporal de incidencias

- [x] Endpoint timeline: /incidences/timeline
- [x] Granularidad dia/semana/mes

## HU7 - Metricas e indices

- [x] KPIs extendidos incluyen reportes faltantes e indice asociado
- [x] Conteo de URLs caidas en ventana de 30 dias

## HU8 - Clasificacion de incidencias

- [x] Categorias operativas registradas en report_incidence
- [x] Filtrado por categoria y severidad en APIs

## HU9 - Identificacion de fallos en DAG

- [x] Endpoint de causa raiz: /dags/{dag_id}/runs/{run_id}/root-cause
- [x] Persistencia de contexto task: upstream/downstream/log_excerpt/token

## HU10 - Interpretacion automatica de fallos

- [x] Regla: missing_reports_count > 0 => incidencia report_not_generated
- [x] Guardado en report_run_expectation y report_incidence

## Criterios de cierre de release

- [x] Unit tests parser/semaforo/deduplicacion/prioridad
- [x] Integration tests repository + endpoints nuevos
- [x] E2E payloads extractor (test_e2e_extractor_payloads.py)
- [x] Smoke post deploy (tests/smoke_release_candidate.py)
- [x] Suite en verde
