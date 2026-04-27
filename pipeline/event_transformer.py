"""
Normalização de eventos do MongoDB para o pipeline Redis.

Três tipos de evento são reconhecidos:
  - preco     → originado de eventos_preco
  - avaliacao → originado de avaliacoes_interacoes (somente tipo=avaliacao)
  - busca     → originado de buscas_usuarios
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _dt_to_ms(dt: Any) -> int:
    if isinstance(dt, datetime):
        return int(dt.timestamp() * 1000)
    if isinstance(dt, (int, float)):
        return int(dt)
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# Normalizadores por tipo de coleção
# ---------------------------------------------------------------------------


def normalize_preco_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    posto_id = str(raw.get("posto_id", ""))
    combustivel = str(raw.get("combustivel", "")).upper()
    preco_novo = float(raw.get("preco_novo", 0.0))
    preco_anterior = float(raw.get("preco_anterior", 0.0))
    variacao_pct = float(raw.get("variacao_pct", 0.0))
    ts_ms = _dt_to_ms(raw.get("ocorrido_em"))
    return {
        "type": "preco",
        "posto_id": posto_id,
        "combustivel": combustivel,
        "preco_novo": preco_novo,
        "preco_anterior": preco_anterior,
        "variacao_pct": variacao_pct,
        "ts_ms": ts_ms,
    }


def normalize_avaliacao_event(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(raw.get("tipo", "")) != "avaliacao" or raw.get("nota") is None:
        return None
    return {
        "type": "avaliacao",
        "posto_id": str(raw.get("posto_id", "")),
        "nota": float(raw.get("nota", 0)),
        "ts_ms": _dt_to_ms(raw.get("created_at")),
    }


def normalize_busca_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "busca",
        "combustivel": str(raw.get("tipo_combustivel", "")).upper(),
        "estado": str(raw.get("estado", "")),
        "cidade": str(raw.get("cidade", "")),
    }


# ---------------------------------------------------------------------------
# Chaves Redis
# ---------------------------------------------------------------------------


def posto_hash_key(posto_id: str) -> str:
    return f"posto:{posto_id}"


def preco_ranking_key(combustivel: str) -> str:
    """Sorted Set: score = preço, member = posto_id (menor score = menor preço)."""
    return f"ranking:precos:{combustivel.lower()}"


def variacao_ranking_key() -> str:
    """Sorted Set: score = |variação %|, member = posto_id (maior = mais volátil)."""
    return "ranking:postos:variacao_pct"


def avaliacao_ranking_key() -> str:
    """Sorted Set: score = nota_media, member = posto_id."""
    return "ranking:postos:avaliacoes"


def busca_combustivel_key() -> str:
    """Sorted Set: score = nº buscas, member = combustivel."""
    return "ranking:buscas:combustivel"


def busca_estado_key() -> str:
    """Sorted Set: score = nº buscas, member = estado."""
    return "ranking:buscas:estado"


def preco_ts_key(posto_id: str, combustivel: str) -> str:
    """Time Series key para evolução de preço de um combustível em um posto."""
    return f"ts:posto:{posto_id}:{combustivel.lower()}"
