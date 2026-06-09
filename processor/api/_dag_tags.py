from __future__ import annotations


def cubes_subquery(dag_alias: str) -> str:
    """Fragmento SQL que agrega los cubos de un DAG como columna ``cubes``.

    Devuelve un subquery correlacionado (string unido por ', ') contra
    ``monitoring.dag_cube``, para usar dentro del SELECT de cualquier endpoint que
    liste DAGs. ``dag_alias`` es el alias de la tabla/vista que aporta ``dag_id`` y
    ``region`` (es una constante del código, nunca input de usuario).
    """
    return (
        "(SELECT string_agg(c.cube_tag, ', ' ORDER BY c.cube_tag) "
        "FROM monitoring.dag_cube c "
        f"WHERE c.dag_id = {dag_alias}.dag_id AND c.region = {dag_alias}.region) AS cubes"
    )
