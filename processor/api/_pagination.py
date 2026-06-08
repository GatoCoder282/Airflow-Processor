from __future__ import annotations

DEFAULT_MAX_LIMIT = 5000


def clamp_pagination(limit: int, offset: int, max_limit: int = DEFAULT_MAX_LIMIT) -> tuple[int, int]:
    """Acota ``limit`` a ``[1, max_limit]`` y ``offset`` a ``>= 0``.

    Evita que valores inválidos (p. ej. ``limit=-1``) lleguen a Postgres y produzcan
    un error 500, y acota consultas sin tope superior. Se usa de forma transversal en
    los endpoints de listado paginados.
    """
    safe_limit = min(max(int(limit), 1), max_limit)
    safe_offset = max(int(offset), 0)
    return safe_limit, safe_offset
