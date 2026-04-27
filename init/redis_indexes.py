"""
Inicializa estruturas Redis para o Radar Combustível.

Ações:
  1. Cria hashes posto:{id} com dados cadastrais de cada posto ativo.
  2. Adiciona postos ao índice geoespacial geo:postos (Redis GEO).
  3. Cria o índice RediSearch idx:postos sobre os hashes posto:*.

Deve ser executado uma única vez após mongo_seed.py e antes de iniciar
o consumer (que fará o backfill de preços, avaliações e buscas).

Uso:
  python init/redis_indexes.py
"""

import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.commands.search.field import GeoField, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DB_NAME = os.getenv("DB_NAME", "radar_combustivel")


def seed_posto_hashes(db, redis: Redis) -> int:
    """Cria hash posto:{id} + adiciona à GEO key para cada posto ativo."""
    pipe = redis.pipeline(transaction=False)
    count = 0

    for posto in db.postos.find({"ativo": True}):
        pid = str(posto["_id"])
        endereco = posto.get("endereco", {})
        coords = posto.get("location", {}).get("coordinates", [0.0, 0.0])
        lng, lat = float(coords[0]), float(coords[1])

        pipe.hset(
            f"posto:{pid}",
            mapping={
                "posto_id": pid,
                "nome_fantasia": posto.get("nome_fantasia", ""),
                "bandeira": posto.get("bandeira", ""),
                "estado": endereco.get("estado", ""),
                "cidade": endereco.get("cidade", ""),
                "bairro": endereco.get("bairro", ""),
                "ativo": "1",
                "location": f"{lng},{lat}",
                "nota_media": "0",
                "rating_sum": "0",
                "rating_count": "0",
                "preco_gasolina_comum": "0",
                "preco_gasolina_aditivada": "0",
                "preco_etanol": "0",
                "preco_diesel_s10": "0",
                "preco_diesel_comum": "0",
                "preco_gnv": "0",
            },
        )
        pipe.geoadd("geo:postos", [lng, lat, pid])

        count += 1
        if count % 500 == 0:
            pipe.execute()
            pipe = redis.pipeline(transaction=False)
            print(f"  {count} postos carregados no Redis...", flush=True)

    if count % 500 != 0 or count == 0:
        pipe.execute()

    return count


def create_search_index(redis: Redis) -> None:
    """Cria (ou recria) o índice RediSearch idx:postos."""
    try:
        redis.execute_command("FT.DROPINDEX", "idx:postos", "DD")
    except Exception:
        pass

    redis.ft("idx:postos").create_index(
        fields=[
            TextField("nome_fantasia", weight=2.0),
            TagField("bandeira"),
            TagField("estado"),
            TagField("cidade"),
            TagField("ativo"),
            NumericField("nota_media", sortable=True),
            NumericField("preco_gasolina_comum", sortable=True),
            NumericField("preco_gasolina_aditivada", sortable=True),
            NumericField("preco_etanol", sortable=True),
            NumericField("preco_diesel_s10", sortable=True),
            NumericField("preco_diesel_comum", sortable=True),
            NumericField("preco_gnv", sortable=True),
            GeoField("location"),
        ],
        definition=IndexDefinition(prefix=["posto:"], index_type=IndexType.HASH),
    )


def main() -> int:
    print(f"[REDIS-INIT] Conectando ao MongoDB: {MONGO_URI}")
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        mongo.admin.command("ping")
    except Exception as e:
        print(f"[REDIS-INIT] Erro ao conectar ao MongoDB: {e}", file=sys.stderr)
        return 1

    try:
        redis.ping()
    except Exception as e:
        print(f"[REDIS-INIT] Erro ao conectar ao Redis: {e}", file=sys.stderr)
        return 1

    db = mongo[DB_NAME]

    print("[REDIS-INIT] Populando hashes posto:{id} e geo:postos...")
    n = seed_posto_hashes(db, redis)
    print(f"[REDIS-INIT] {n} postos ativos carregados.")

    print("[REDIS-INIT] Criando índice RediSearch idx:postos...")
    create_search_index(redis)
    print("[REDIS-INIT] idx:postos criado com sucesso.")

    mongo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
