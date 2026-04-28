"""
Consumer MongoDB → Redis — Radar Combustível
============================================
Modo backfill (padrão):
  Processa todos os documentos existentes em eventos_preco,
  avaliacoes_interacoes e buscas_usuarios, populando as
  estruturas Redis antes de assistir o change stream.

Modo streaming (após backfill ou com --skip-backfill):
  Abre três threads paralelas de change stream, uma por coleção.

Uso:
  python pipeline/mongodb_consumer.py
  python pipeline/mongodb_consumer.py --skip-backfill
"""

import argparse
import os
import sys
import threading
import time
from typing import Any, Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError

sys.path.insert(0, os.path.dirname(__file__))
from event_transformer import (
    avaliacao_ranking_key,
    busca_combustivel_key,
    busca_estado_key,
    normalize_avaliacao_event,
    normalize_busca_event,
    normalize_preco_event,
    posto_hash_key,
    preco_ranking_key,
    preco_ts_key,
    variacao_ranking_key,
)

load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DB_NAME = os.getenv("DB_NAME", "radar_combustivel")


# ---------------------------------------------------------------------------
# Helpers Redis
# ---------------------------------------------------------------------------


def _ensure_ts_add(redis: Redis, key: str, ts: int, value: float, labels: Dict[str, str]) -> None:
    """Insere ponto na time series; labels são aplicadas na criação automática pelo Redis Stack."""
    flat_labels = sum(([k, v] for k, v in labels.items()), [])
    redis.execute_command(
        "TS.ADD", key, ts, value,
        "ON_DUPLICATE", "LAST",
        "RETENTION", 0,
        "DUPLICATE_POLICY", "LAST",
        "LABELS", *flat_labels,
    )


# ---------------------------------------------------------------------------
# Handlers por tipo de evento
# ---------------------------------------------------------------------------


def apply_preco(redis: Redis, event: Dict[str, Any]) -> None:
    posto_id = event["posto_id"]
    combustivel = event["combustivel"]
    preco = event["preco_novo"]
    variacao = abs(event["variacao_pct"])
    ts = event["ts_ms"]

    h_key = posto_hash_key(posto_id)
    field_preco = f"preco_{combustivel.lower()}"

    redis.hset(h_key, mapping={field_preco: round(preco, 3), "atualizado_em": ts})

    # Sorted Set de ranking por menor preço (score = preço)
    redis.zadd(preco_ranking_key(combustivel), {posto_id: preco})

    # Sorted Set de maior variação recente (score = |variação %|)
    redis.zadd(variacao_ranking_key(), {posto_id: variacao})

    # Time Series para evolução de preço
    _ensure_ts_add(
        redis,
        preco_ts_key(posto_id, combustivel),
        ts,
        preco,
        {"posto_id": posto_id, "combustivel": combustivel.lower()},
    )


def apply_avaliacao(redis: Redis, event: Dict[str, Any]) -> None:
    posto_id = event["posto_id"]
    nota = event["nota"]
    h_key = posto_hash_key(posto_id)

    redis.hincrbyfloat(h_key, "rating_sum", nota)
    redis.hincrby(h_key, "rating_count", 1)

    rating_sum = float(redis.hget(h_key, "rating_sum") or 0.0)
    rating_count = int(redis.hget(h_key, "rating_count") or 1)
    nota_media = round(rating_sum / max(rating_count, 1), 2)

    redis.hset(h_key, "nota_media", nota_media)
    redis.zadd(avaliacao_ranking_key(), {posto_id: nota_media})


def apply_busca(redis: Redis, event: Dict[str, Any]) -> None:
    if event["combustivel"]:
        redis.zincrby(busca_combustivel_key(), 1, event["combustivel"])
    if event["estado"]:
        redis.zincrby(busca_estado_key(), 1, event["estado"])


# ---------------------------------------------------------------------------
# Handlers de documento bruto (despachados pelos change streams)
# ---------------------------------------------------------------------------


def handle_preco_doc(redis: Redis, doc: Dict[str, Any]) -> None:
    apply_preco(redis, normalize_preco_event(doc))


def handle_avaliacao_doc(redis: Redis, doc: Dict[str, Any]) -> None:
    event = normalize_avaliacao_event(doc)
    if event:
        apply_avaliacao(redis, event)


def handle_busca_doc(redis: Redis, doc: Dict[str, Any]) -> None:
    apply_busca(redis, normalize_busca_event(doc))


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


def backfill(db, redis: Redis) -> None:
    print("[BACKFILL] Processando eventos_preco (ordenado por ocorrido_em)...")
    count = 0
    for doc in db.eventos_preco.find({}).sort("ocorrido_em", 1):
        handle_preco_doc(redis, doc)
        count += 1
        if count % 2000 == 0:
            print(f"[BACKFILL]   eventos_preco: {count} processados...")
    print(f"[BACKFILL] eventos_preco: {count} total.")

    print("[BACKFILL] Processando avaliacoes_interacoes...")
    count = 0
    for doc in db.avaliacoes_interacoes.find(
        {"tipo": "avaliacao", "nota": {"$ne": None}}
    ).sort("created_at", 1):
        handle_avaliacao_doc(redis, doc)
        count += 1
    print(f"[BACKFILL] avaliacoes_interacoes: {count} avaliações processadas.")

    print("[BACKFILL] Processando buscas_usuarios...")
    count = 0
    for doc in db.buscas_usuarios.find({}):
        handle_busca_doc(redis, doc)
        count += 1
    print(f"[BACKFILL] buscas_usuarios: {count} buscas processadas.")

    print("[BACKFILL] Concluído.")


# ---------------------------------------------------------------------------
# Change Stream por coleção (thread)
# ---------------------------------------------------------------------------


def watch_collection(db, redis: Redis, collection_name: str, handler) -> None:
    col = db[collection_name]
    print(f"[STREAM] Assistindo {collection_name}...")
    while True:
        try:
            with col.watch(
                [{"$match": {"operationType": "insert"}}],
                full_document="updateLookup",
            ) as stream:
                for change in stream:
                    doc = change["fullDocument"]
                    handler(redis, doc)
                    print(f"[STREAM] {collection_name} → {change['operationType']} processado.")
        except Exception as exc:
            print(f"[STREAM] {collection_name} — reconectando após erro: {exc}")
            time.sleep(2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline MongoDB → Redis | Radar Combustível")
    parser.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Não processa documentos já existentes (apenas change stream).",
    )
    args = parser.parse_args()

    mongo = MongoClient(MONGO_URI)
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    db = mongo[DB_NAME]

    if not args.skip_backfill:
        backfill(db, redis)

    streams = [
        ("eventos_preco",          handle_preco_doc),
        ("avaliacoes_interacoes",  handle_avaliacao_doc),
        ("buscas_usuarios",        handle_busca_doc),
    ]

    threads = [
        threading.Thread(
            target=watch_collection,
            args=(db, redis, col_name, handler),
            daemon=True,
            name=f"stream-{col_name}",
        )
        for col_name, handler in streams
    ]

    for t in threads:
        t.start()

    print("[CONSUMER] Pipeline ativo. Aguardando eventos em tempo real...")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("[CONSUMER] Encerrando.")


if __name__ == "__main__":
    main()
