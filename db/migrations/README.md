# Migraciones de base de datos (esquema `monitoring`)

Las tablas y vistas viven directamente en PostgreSQL (no hay un ORM ni DDL
versionado del esquema completo). Esta carpeta agrupa los scripts SQL de cambio
que acompañan a los cambios de código, para ejecutarlos manualmente.

## Convención de nombres

`YYYY-MM-DD_NNN_descripcion.sql` — orden cronológico + secuencia.

## Cómo ejecutar

```bash
psql "$DATABASE_URL" -f db/migrations/2026-06-05_001_remove_incidences_public_schema.sql
```

## Índice

| Script | Qué hace |
|--------|----------|
| `2026-06-05_001_remove_incidences_public_schema.sql` | Elimina `incidence_timeline` (mat. view) y `report_incidence`. Incluye, comentada, una FASE 2 para `report_run_expectation` que requiere revisar/recrear antes la vista `kpi_summary`. |
| `2026-06-08_002_dag_cube_many_to_many.sql` | Crea `monitoring.dag_cube` (relación muchos-a-muchos DAG→cubos) + índice + FK + backfill desde `dag_catalog.cube_tag`. |

> **Nota:** el código del backend ya no lee ni escribe estos objetos, así que la
> aplicación funciona aunque la migración aún no se haya corrido. Correrla solo
> limpia objetos huérfanos de la base.
