"""
Dashboard Streamlit — Radar Combustível
=======================================
Visualização em tempo real das estruturas Redis populadas pelo pipeline
MongoDB → Redis.

Abas:
  1. Preços        — ranking de menores preços por combustível
  2. Variações     — postos com maior variação recente de preço
  3. Avaliações    — postos mais bem avaliados
  4. Buscas        — volume de buscas por combustível e estado
  5. Evolução      — séries temporais de preço por posto/combustível
  6. Busca Avançada — pesquisa com filtros via RediSearch

Uso:
  streamlit run dashboard/app.py
"""

import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

load_dotenv(".env.local")
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

COMBUSTIVEIS = [
    "GASOLINA_COMUM",
    "GASOLINA_ADITIVADA",
    "ETANOL",
    "DIESEL_S10",
    "DIESEL_COMUM",
    "GNV",
]

COMBUSTIVEL_LABELS = {
    "GASOLINA_COMUM": "Gasolina Comum",
    "GASOLINA_ADITIVADA": "Gasolina Aditivada",
    "ETANOL": "Etanol",
    "DIESEL_S10": "Diesel S10",
    "DIESEL_COMUM": "Diesel Comum",
    "GNV": "GNV",
}


@st.cache_resource
def get_redis() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def posto_info(redis: Redis, posto_id: str) -> Dict[str, str]:
    h = redis.hgetall(f"posto:{posto_id}")
    return h if h else {"nome_fantasia": posto_id, "bandeira": "-", "cidade": "-", "estado": "-"}


def resolve_postos(redis: Redis, ids: List[str]) -> Dict[str, Dict[str, str]]:
    return {pid: posto_info(redis, pid) for pid in ids}


# ---------------------------------------------------------------------------
# Funções de leitura Redis
# ---------------------------------------------------------------------------


def get_ranking_preco(redis: Redis, combustivel: str, n: int = 15) -> List[Tuple[str, float]]:
    return redis.zrange(f"ranking:precos:{combustivel.lower()}", 0, n - 1, withscores=True)


def get_ranking_variacao(redis: Redis, n: int = 15) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:postos:variacao_pct", 0, n - 1, withscores=True)


def get_ranking_avaliacoes(redis: Redis, n: int = 15) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:postos:avaliacoes", 0, n - 1, withscores=True)


def get_buscas_combustivel(redis: Redis) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:buscas:combustivel", 0, -1, withscores=True)


def get_buscas_estado(redis: Redis) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:buscas:estado", 0, -1, withscores=True)


def get_ts_range(redis: Redis, posto_id: str, combustivel: str) -> List[Tuple[int, float]]:
    key = f"ts:posto:{posto_id}:{combustivel.lower()}"
    try:
        return redis.execute_command("TS.RANGE", key, "-", "+")
    except Exception:
        return []


def get_ts_mrange_avg(redis: Redis, combustivel: str, bucket_ms: int = 86_400_000) -> list:
    """Média diária de preço de todos os postos para um combustível."""
    try:
        return redis.execute_command(
            "TS.MRANGE", "-", "+",
            "AGGREGATION", "avg", bucket_ms,
            "FILTER", f"combustivel={combustivel.lower()}",
        )
    except Exception:
        return []


def get_redisearch(
    redis: Redis,
    estado: str = "",
    bandeira: str = "",
    nota_min: float = 0.0,
    preco_max: float = 9.99,
    combustivel_filtro: str = "GASOLINA_COMUM",
    n: int = 20,
) -> Any:
    parts = []
    if estado:
        parts.append(f"@estado:{{{estado}}}")
    if bandeira:
        parts.append(f"@bandeira:{{{bandeira.replace(' ', chr(92) + ' ')}}}")
    query_text = " ".join(parts) if parts else "*"
    field_preco = f"preco_{combustivel_filtro.lower()}"
    query = (
        Query(query_text)
        .add_filter(NumericFilter("nota_media", nota_min, 5.0))
        .add_filter(NumericFilter(field_preco, 0.01, preco_max))
        .sort_by(field_preco, asc=True)
        .paging(0, n)
    )
    try:
        return redis.ft("idx:postos").search(query)
    except Exception as exc:
        return None


def get_summary_metrics(redis: Redis) -> Dict[str, int]:
    return {
        "postos": redis.zcard("ranking:postos:avaliacoes") + redis.zcard("ranking:postos:variacao_pct"),
        "eventos_preco": sum(redis.zcard(f"ranking:precos:{c.lower()}") for c in COMBUSTIVEIS),
        "buscas": int(sum(s for _, s in get_buscas_estado(redis)) if get_buscas_estado(redis) else 0),
        "avaliacoes": redis.zcard("ranking:postos:avaliacoes"),
    }


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Radar Combustível — Dashboard",
    page_icon="⛽",
    layout="wide",
)

st.title("⛽ Radar Combustível — Dashboard Redis")
st.caption("Pipeline MongoDB → Redis | Visualização em tempo real das estruturas de serving.")

redis = get_redis()

# Sidebar
with st.sidebar:
    st.header("Configurações")
    auto_refresh = st.toggle("Auto-refresh", value=False)
    refresh_seconds = st.number_input("Intervalo (s)", min_value=5, max_value=120, value=15, step=5)
    n_items = st.number_input("Itens por ranking", min_value=5, max_value=30, value=10, step=5)
    st.divider()
    st.caption(f"Redis: {REDIS_HOST}:{REDIS_PORT}")

# Métricas no topo
metrics = get_summary_metrics(redis)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Postos indexados", redis.zcard("ranking:postos:avaliacoes") or "—")
c2.metric("Postos com preço registrado", redis.zcard("ranking:precos:gasolina_comum") or "—")
c3.metric("Total de buscas", f"{int(metrics['buscas']):,}")
c4.metric("Postos avaliados", redis.zcard("ranking:postos:avaliacoes") or "—")

st.divider()

# Abas principais
tab_precos, tab_variacoes, tab_aval, tab_buscas, tab_ts, tab_search = st.tabs([
    "🏷️ Preços",
    "📊 Variações",
    "⭐ Avaliações",
    "🔍 Buscas",
    "📈 Evolução de Preços",
    "🔎 Busca Avançada",
])


# ─── Aba 1: Preços ─────────────────────────────────────────────────────────
with tab_precos:
    st.subheader("Top postos com menores preços por combustível")
    combustivel_sel = st.selectbox(
        "Combustível",
        COMBUSTIVEIS,
        format_func=lambda x: COMBUSTIVEL_LABELS[x],
        key="sel_preco",
    )

    dados_preco = get_ranking_preco(redis, combustivel_sel, int(n_items))

    if not dados_preco:
        st.info("Nenhum dado disponível. Execute o pipeline para popular o Redis.")
    else:
        infos = resolve_postos(redis, [pid for pid, _ in dados_preco])
        rows = [
            {
                "Posto": infos[pid].get("nome_fantasia", pid),
                "Bandeira": infos[pid].get("bandeira", "-"),
                "Cidade": infos[pid].get("cidade", "-"),
                "Estado": infos[pid].get("estado", "-"),
                f"Preço {COMBUSTIVEL_LABELS[combustivel_sel]} (R$)": round(preco, 3),
            }
            for pid, preco in dados_preco
        ]
        df = pd.DataFrame(rows)
        col_preco = f"Preço {COMBUSTIVEL_LABELS[combustivel_sel]} (R$)"

        col_left, col_right = st.columns([3, 2])
        with col_left:
            fig = px.bar(
                df.sort_values(col_preco, ascending=False),
                x=col_preco,
                y="Posto",
                orientation="h",
                color=col_preco,
                color_continuous_scale="RdYlGn_r",
                title=f"Top {int(n_items)} menores preços — {COMBUSTIVEL_LABELS[combustivel_sel]}",
            )
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, width="stretch")
        with col_right:
            st.dataframe(df, width="stretch", hide_index=True)


# ─── Aba 2: Variações ──────────────────────────────────────────────────────
with tab_variacoes:
    st.subheader("Postos com maior variação recente de preço")
    dados_var = get_ranking_variacao(redis, int(n_items))

    if not dados_var:
        st.info("Nenhum dado disponível.")
    else:
        infos = resolve_postos(redis, [pid for pid, _ in dados_var])
        rows = [
            {
                "Posto": infos[pid].get("nome_fantasia", pid),
                "Bandeira": infos[pid].get("bandeira", "-"),
                "Cidade": infos[pid].get("cidade", "-"),
                "Estado": infos[pid].get("estado", "-"),
                "Variação (%)": round(var, 2),
            }
            for pid, var in dados_var
        ]
        df = pd.DataFrame(rows)

        col_left, col_right = st.columns([3, 2])
        with col_left:
            fig = px.bar(
                df.sort_values("Variação (%)", ascending=True),
                x="Variação (%)",
                y="Posto",
                orientation="h",
                color="Variação (%)",
                color_continuous_scale="Reds",
                title=f"Top {int(n_items)} maiores variações de preço (%)",
            )
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, width="stretch")
        with col_right:
            st.dataframe(df, width="stretch", hide_index=True)


# ─── Aba 3: Avaliações ─────────────────────────────────────────────────────
with tab_aval:
    st.subheader("Postos mais bem avaliados")
    dados_aval = get_ranking_avaliacoes(redis, int(n_items))

    if not dados_aval:
        st.info("Nenhum dado disponível.")
    else:
        infos = resolve_postos(redis, [pid for pid, _ in dados_aval])
        rows = []
        for pid, nota in dados_aval:
            h = infos[pid]
            total = redis.hget(f"posto:{pid}", "rating_count") or "0"
            rows.append({
                "Posto": h.get("nome_fantasia", pid),
                "Bandeira": h.get("bandeira", "-"),
                "Cidade": h.get("cidade", "-"),
                "Estado": h.get("estado", "-"),
                "Nota Média": round(nota, 2),
                "Total Avaliações": int(total),
            })
        df = pd.DataFrame(rows)

        col_left, col_right = st.columns([3, 2])
        with col_left:
            fig = px.bar(
                df.sort_values("Nota Média", ascending=True),
                x="Nota Média",
                y="Posto",
                orientation="h",
                color="Nota Média",
                color_continuous_scale="YlGn",
                range_x=[0, 5],
                title=f"Top {int(n_items)} por nota média",
            )
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, width="stretch")
        with col_right:
            st.dataframe(df, width="stretch", hide_index=True)


# ─── Aba 4: Buscas ─────────────────────────────────────────────────────────
with tab_buscas:
    st.subheader("Volume de buscas na plataforma")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Por combustível**")
        dados_comb = get_buscas_combustivel(redis)
        if not dados_comb:
            st.info("Sem dados de buscas por combustível.")
        else:
            df_c = pd.DataFrame(
                [(COMBUSTIVEL_LABELS.get(c, c), int(v)) for c, v in dados_comb],
                columns=["Combustível", "Buscas"],
            )
            fig = px.pie(df_c, names="Combustível", values="Buscas", title="Distribuição por combustível")
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df_c, width="stretch", hide_index=True)

    with col_b:
        st.markdown("**Por estado (UF)**")
        dados_est = get_buscas_estado(redis)
        if not dados_est:
            st.info("Sem dados de buscas por estado.")
        else:
            df_e = pd.DataFrame(
                [(uf, int(v)) for uf, v in dados_est],
                columns=["Estado", "Buscas"],
            )
            fig = px.bar(
                df_e.sort_values("Buscas", ascending=True),
                x="Buscas",
                y="Estado",
                orientation="h",
                color="Buscas",
                color_continuous_scale="Blues",
                title="Volume de buscas por estado",
            )
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, width="stretch")


# ─── Aba 5: Evolução de Preços ─────────────────────────────────────────────
with tab_ts:
    st.subheader("Evolução de preço por posto e combustível")

    col_ts1, col_ts2, col_ts3 = st.columns(3)
    with col_ts1:
        ts_posto_id = st.text_input("ID do posto (24 hex chars)", key="ts_posto")
    with col_ts2:
        ts_combustivel = st.selectbox(
            "Combustível",
            COMBUSTIVEIS,
            format_func=lambda x: COMBUSTIVEL_LABELS[x],
            key="ts_comb",
        )
    with col_ts3:
        st.write("")
        st.write("")
        buscar_ts = st.button("Buscar série temporal")

    if buscar_ts and ts_posto_id:
        series = get_ts_range(redis, ts_posto_id.strip(), ts_combustivel)
        if not series:
            st.warning(f"Nenhum dado em `ts:posto:{ts_posto_id.strip()}:{ts_combustivel.lower()}`.")
        else:
            df_ts = pd.DataFrame(series, columns=["ts_ms", "preco"])
            df_ts["preco"] = df_ts["preco"].astype(float)
            df_ts["datetime"] = pd.to_datetime(df_ts["ts_ms"].astype(int), unit="ms")
            nome = redis.hget(f"posto:{ts_posto_id.strip()}", "nome_fantasia") or ts_posto_id.strip()
            fig = px.line(
                df_ts,
                x="datetime",
                y="preco",
                markers=True,
                title=f"Preço {COMBUSTIVEL_LABELS[ts_combustivel]} — {nome}",
                labels={"preco": "Preço (R$/L)", "datetime": "Data"},
            )
            st.plotly_chart(fig, width="stretch")

    st.divider()
    st.markdown("**Tendência média diária por combustível (todos os postos)**")
    ts_comb_geral = st.selectbox(
        "Combustível (visão geral)",
        COMBUSTIVEIS,
        format_func=lambda x: COMBUSTIVEL_LABELS[x],
        key="ts_geral",
    )

    mrange_data = get_ts_mrange_avg(redis, ts_comb_geral, bucket_ms=86_400_000)
    if not mrange_data:
        st.info("Sem dados de série temporal agregada. Execute o pipeline para popular.")
    else:
        all_points: List[Dict] = []
        for entry in mrange_data[:20]:
            key_name = entry[0]
            posto_part = key_name.split(":")
            pid = posto_part[2] if len(posto_part) > 2 else key_name
            for ts_ms, val in entry[2]:
                all_points.append({
                    "posto_id": pid,
                    "datetime": datetime.fromtimestamp(int(ts_ms) / 1000),
                    "preco": float(val),
                })
        if all_points:
            df_mrange = pd.DataFrame(all_points)
            df_avg = df_mrange.groupby("datetime")["preco"].mean().reset_index()
            df_avg.columns = ["datetime", "preco_medio"]
            fig = px.line(
                df_avg,
                x="datetime",
                y="preco_medio",
                markers=True,
                title=f"Preço médio diário — {COMBUSTIVEL_LABELS[ts_comb_geral]} (amostra de postos)",
                labels={"preco_medio": "Preço Médio (R$/L)", "datetime": "Data"},
            )
            st.plotly_chart(fig, width="stretch")


# ─── Aba 6: Busca Avançada ─────────────────────────────────────────────────
with tab_search:
    st.subheader("Busca avançada de postos (RediSearch)")

    f1, f2, f3, f4, f5 = st.columns(5)
    with f1:
        s_estado = st.selectbox("Estado", [""] + ["SP", "RJ", "MG", "PR", "RS", "BA", "PE", "CE", "DF", "GO"])
    with f2:
        s_bandeira = st.selectbox(
            "Bandeira",
            ["", "Ipiranga", "Shell", "BR", "Raízen", "Ale", "Boxter", "Petrobras", "Rede independente"],
        )
    with f3:
        s_nota_min = st.slider("Nota mínima", 0.0, 5.0, 0.0, 0.5)
    with f4:
        s_preco_max = st.slider("Preço máx (R$/L)", 4.0, 10.0, 9.0, 0.1)
    with f5:
        s_combustivel = st.selectbox(
            "Combustível filtro de preço",
            COMBUSTIVEIS,
            format_func=lambda x: COMBUSTIVEL_LABELS[x],
            key="s_comb",
        )

    result = get_redisearch(
        redis,
        estado=s_estado,
        bandeira=s_bandeira,
        nota_min=s_nota_min,
        preco_max=s_preco_max,
        combustivel_filtro=s_combustivel,
        n=int(n_items),
    )

    if result is None:
        st.error("Erro ao consultar o índice RediSearch. Verifique se o pipeline foi executado.")
    elif result.total == 0:
        st.info("Nenhum posto encontrado para os filtros aplicados.")
    else:
        st.caption(f"{result.total} resultado(s) encontrado(s).")
        field_preco = f"preco_{s_combustivel.lower()}"
        rows = []
        for doc in result.docs:
            preco_val = float(getattr(doc, field_preco, 0) or 0)
            rows.append({
                "Posto": getattr(doc, "nome_fantasia", doc.id),
                "Bandeira": getattr(doc, "bandeira", "-"),
                "Cidade": getattr(doc, "cidade", "-"),
                "Estado": getattr(doc, "estado", "-"),
                f"Preço {COMBUSTIVEL_LABELS[s_combustivel]} (R$)": round(preco_val, 3),
                "Nota Média": float(getattr(doc, "nota_media", 0) or 0),
            })
        df_search = pd.DataFrame(rows)
        st.dataframe(df_search, width="stretch", hide_index=True)


# Auto-refresh
if auto_refresh:
    time.sleep(int(refresh_seconds))
    st.rerun()
