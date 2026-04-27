"""
Consultas demonstrativas Redis — Radar Combustível
==================================================
Exibe, em loop, as principais leituras servidas pela camada Redis:

  1. Top 10 menores preços por combustível (Sorted Set)
  2. Top 10 postos com maior variação recente (Sorted Set)
  3. Top 10 postos mais bem avaliados (Sorted Set)
  4. Buscas por combustível e por estado (Sorted Set)
  5. Postos próximos a um ponto (GEO)
  6. Busca por estado + bandeira (RediSearch)

Uso:
  python queries/redis_reader.py
  python queries/redis_reader.py --once   # imprime uma vez e encerra
"""

import argparse
import os
import time
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

load_dotenv(".env.local")
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

COMBUSTIVEIS = (
    "GASOLINA_COMUM",
    "GASOLINA_ADITIVADA",
    "ETANOL",
    "DIESEL_S10",
    "DIESEL_COMUM",
    "GNV",
)


def get_redis() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def posto_nome(redis: Redis, posto_id: str) -> str:
    nome = redis.hget(f"posto:{posto_id}", "nome_fantasia")
    return nome or posto_id


def posto_info(redis: Redis, posto_id: str) -> str:
    h = redis.hgetall(f"posto:{posto_id}")
    if not h:
        return posto_id
    return f"{h.get('nome_fantasia', posto_id)} ({h.get('bandeira', '-')}) — {h.get('cidade', '-')}/{h.get('estado', '-')}"


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------


def top_menor_preco(redis: Redis, combustivel: str, n: int = 10) -> List[Tuple[str, float]]:
    """Sorted Set: menor score = menor preço."""
    return redis.zrange(f"ranking:precos:{combustivel.lower()}", 0, n - 1, withscores=True)


def top_variacao(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    """Sorted Set: maior score = maior variação absoluta (%)."""
    return redis.zrevrange("ranking:postos:variacao_pct", 0, n - 1, withscores=True)


def top_avaliados(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    """Sorted Set: maior score = melhor nota média."""
    return redis.zrevrange("ranking:postos:avaliacoes", 0, n - 1, withscores=True)


def top_buscas_combustivel(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:buscas:combustivel", 0, n - 1, withscores=True)


def top_buscas_estado(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:buscas:estado", 0, n - 1, withscores=True)


def postos_proximos(
    redis: Redis,
    lng: float,
    lat: float,
    raio_km: float = 50,
    n: int = 5,
) -> list:
    """GEO: postos dentro do raio em km."""
    return redis.geosearch(
        "geo:postos",
        longitude=lng,
        latitude=lat,
        radius=raio_km,
        unit="km",
        count=n,
        sort="ASC",
        withcoord=True,
        withdist=True,
    )


def busca_redisearch(
    redis: Redis,
    estado: Optional[str] = None,
    bandeira: Optional[str] = None,
    nota_min: float = 0.0,
    n: int = 10,
) -> list:
    """RediSearch: filtro por estado, bandeira e nota mínima."""
    parts = []
    if estado:
        parts.append(f"@estado:{{{estado}}}")
    if bandeira:
        escaped = bandeira.replace(" ", "\\ ")
        parts.append(f"@bandeira:{{{escaped}}}")
    query_text = " ".join(parts) if parts else "*"
    query = (
        Query(query_text)
        .add_filter(NumericFilter("nota_media", nota_min, 5))
        .sort_by("nota_media", asc=False)
        .paging(0, n)
    )
    try:
        return redis.ft("idx:postos").search(query).docs
    except Exception as exc:
        print(f"[READER] RediSearch erro: {exc}")
        return []


def preco_ts(redis: Redis, posto_id: str, combustivel: str) -> list:
    """Time Series: últimos pontos de preço para um posto/combustível."""
    key = f"ts:posto:{posto_id}:{combustivel.lower()}"
    try:
        return redis.execute_command("TS.RANGE", key, "-", "+")
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------


def sep(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print("=" * 70)


def run_queries(redis: Redis) -> None:
    sep("TOP 10 MENORES PREÇOS — GASOLINA COMUM")
    for i, (pid, score) in enumerate(top_menor_preco(redis, "GASOLINA_COMUM"), 1):
        print(f"  {i:02d}. R$ {score:.3f}  |  {posto_info(redis, pid)}")

    sep("TOP 10 MENORES PREÇOS — ETANOL")
    for i, (pid, score) in enumerate(top_menor_preco(redis, "ETANOL"), 1):
        print(f"  {i:02d}. R$ {score:.3f}  |  {posto_info(redis, pid)}")

    sep("TOP 10 POSTOS COM MAIOR VARIAÇÃO RECENTE DE PREÇO")
    for i, (pid, score) in enumerate(top_variacao(redis), 1):
        print(f"  {i:02d}. {score:.2f}%  |  {posto_info(redis, pid)}")

    sep("TOP 10 POSTOS MAIS BEM AVALIADOS")
    for i, (pid, score) in enumerate(top_avaliados(redis), 1):
        print(f"  {i:02d}. ★ {score:.2f}  |  {posto_info(redis, pid)}")

    sep("VOLUME DE BUSCAS POR COMBUSTÍVEL")
    for combustivel, count in top_buscas_combustivel(redis):
        print(f"  {combustivel:<25} {int(count):>6} buscas")

    sep("VOLUME DE BUSCAS POR ESTADO")
    for estado, count in top_buscas_estado(redis):
        print(f"  {estado:<5} {int(count):>6} buscas")

    sep("POSTOS PRÓXIMOS AO CENTRO DE SP (50 km)")
    sp_lng, sp_lat = -46.6333, -23.5505
    for item in postos_proximos(redis, sp_lng, sp_lat, raio_km=50):
        pid, dist, coords = item[0], item[1], item[2]
        print(f"  {dist:.1f} km  |  {posto_info(redis, pid)}")

    sep("BUSCA REDISEARCH: Ipiranga em SP, nota ≥ 3.0")
    for doc in busca_redisearch(redis, estado="SP", bandeira="Ipiranga", nota_min=3.0, n=5):
        print(
            f"  {getattr(doc, 'nome_fantasia', doc.id)}"
            f" | ★ {getattr(doc, 'nota_media', '-')}"
            f" | {getattr(doc, 'cidade', '-')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Executa uma vez e encerra.")
    args = parser.parse_args()

    redis = get_redis()
    print("[READER] Radar Combustível — Consultas Redis")

    if args.once:
        run_queries(redis)
        return

    while True:
        run_queries(redis)
        print("\n[READER] Aguardando 10s para próxima leitura...")
        time.sleep(10)


if __name__ == "__main__":
    main()
