"""
BQ Viewer – raport porównawczy (sklep × kategoria)
Trzy osobne zapytania = każde tylko tyle dni ile potrzeba.
"""

import os, json, pathlib
from datetime import date, timedelta

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# ─────────────────────────────────────────
# KONFIGURACJA
# ─────────────────────────────────────────

YESTERDAY  = date.today() - timedelta(1)

CATEGORY_COLS = ["gender", "season", "seasonality", "type"]
IMPORT_TS_COL = "load_ts_utc"
DATE_COL      = "event_date"
SHOP_COL      = "shop_name"
INDEX_COL     = "index"


# ─────────────────────────────────────────
# CREDENTIALS + CLIENT
# ─────────────────────────────────────────

@st.cache_resource
def get_credentials():
    creds_path = pathlib.Path(__file__).parent / "credentials.json"
    if creds_path.exists():
        sa_info = json.loads(creds_path.read_text())
    else:
        sa_info = dict(st.secrets["bigquery_credentials"])
    return service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def get_client(project: str) -> bigquery.Client:
    return bigquery.Client(credentials=get_credentials(), project=project)


def parse_project(table: str) -> str:
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError(f"Zły format tabeli: '{table}'. Oczekiwany: projekt.dataset.tabela")
    return parts[0]


# ─────────────────────────────────────────
# COST ESTIMATE (DRY RUN) – dla jednego okresu
# ─────────────────────────────────────────

def estimate_cost_single(table: str, start: date, end: date) -> dict:
    try:
        project = parse_project(table)
        client  = get_client(project)
        query   = build_query(table, start, end)
        cfg     = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job     = client.query(query, job_config=cfg)
        gb      = job.total_bytes_processed / 1e9
        cost    = gb / 1000 * 5
        return {"gb": round(gb, 4), "cost_usd": round(cost, 6), "ok": True}
    except Exception as e:
        return {"gb": 0, "cost_usd": 0, "ok": False, "error": str(e)}


def estimate_cost_all(table: str, periods: list) -> dict:
    """Sumuje dry run dla trzech osobnych zapytań."""
    total_gb   = 0
    total_cost = 0
    for start, end in periods:
        est = estimate_cost_single(table, start, end)
        if not est["ok"]:
            return est
        total_gb   += est["gb"]
        total_cost += est["cost_usd"]
    return {"gb": round(total_gb, 4), "cost_usd": round(total_cost, 6), "ok": True}


# ─────────────────────────────────────────
# LISTA SKLEPÓW – lekkie zapytanie przy starcie
# ─────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_shops(_creds_hash: str, table: str) -> list:
    """Pobiera unikalną listę sklepów – bardzo małe zapytanie."""
    project = parse_project(table)
    client  = bigquery.Client(credentials=get_credentials(), project=project)
    query   = f"SELECT DISTINCT {SHOP_COL} FROM `{table}` ORDER BY {SHOP_COL}"
    job     = client.query(query)
    df      = job.result().to_dataframe()
    return sorted(df[SHOP_COL].dropna().tolist())


# ─────────────────────────────────────────
# QUERY
# ─────────────────────────────────────────

def build_query(table: str, start: date, end: date) -> str:
    extra_cols = ", ".join([INDEX_COL] + CATEGORY_COLS)
    return f"""
    SELECT {SHOP_COL}, {DATE_COL}, {extra_cols}
    FROM `{table}`
    WHERE {DATE_COL} BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
    """


@st.cache_data(ttl=600, show_spinner=False)
def fetch_period(_creds_hash: str, table: str, start: date, end: date) -> pd.DataFrame:
    """Pobiera tylko jeden okres. Cache 10 min."""
    project = parse_project(table)
    client  = bigquery.Client(credentials=get_credentials(), project=project)
    job     = client.query(build_query(table, start, end))
    df      = job.result().to_dataframe()
    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL]).dt.date
    return df


# ─────────────────────────────────────────
# ZAKRESY DAT – trzy okresy
# ─────────────────────────────────────────

def get_periods(preset: str, custom_start: date = None, custom_end: date = None):
    if preset == "Ostatni tydzień":
        end   = YESTERDAY
        start = end - timedelta(6)
    elif preset == "Ostatnie 14 dni":
        end   = YESTERDAY
        start = end - timedelta(13)
    elif preset == "Ostatnie 30 dni":
        end   = YESTERDAY
        start = end - timedelta(29)
    else:
        start = custom_start or (YESTERDAY - timedelta(6))
        end   = custom_end   or YESTERDAY

    n = (end - start).days + 1
    prev_week = (start - timedelta(n), end - timedelta(n))
    prev_year = (
        start.replace(year=start.year - 1),
        end.replace(year=end.year - 1),
    )
    return (start, end), prev_week, prev_year


# ─────────────────────────────────────────
# AGREGACJA – unikalne indeksy w całym okresie
# ─────────────────────────────────────────

def count_products(df: pd.DataFrame) -> int:
    """Liczba unikalnych indeksów produktów w okresie."""
    if INDEX_COL in df.columns:
        return df[INDEX_COL].nunique()
    return len(df)


def build_summary(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """Agregacja po kategorii – unikalne indeksy w całym okresie."""
    if group_col not in df.columns:
        return pd.DataFrame(columns=[group_col, "produkty"])
    if INDEX_COL in df.columns:
        out = (
            df.groupby(group_col)[INDEX_COL]
            .nunique()
            .reset_index()
            .rename(columns={INDEX_COL: "produkty"})
        )
    else:
        out = df.groupby(group_col).size().reset_index(name="produkty")
    return out.sort_values("produkty", ascending=False)


def compare_periods(df_cur, df_prev, group_col) -> pd.DataFrame:
    cur    = build_summary(df_cur,  group_col).rename(columns={"produkty": "bieżący"})
    prev   = build_summary(df_prev, group_col).rename(columns={"produkty": "poprzedni"})
    merged = cur.merge(prev, on=group_col, how="outer").fillna(0)
    merged["bieżący"]   = merged["bieżący"].astype(int)
    merged["poprzedni"] = merged["poprzedni"].astype(int)
    merged["zmiana"]    = merged["bieżący"] - merged["poprzedni"]
    merged["zmiana %"]  = merged.apply(
        lambda r: f"{r['zmiana']/r['poprzedni']*100:+.1f}%"
        if r["poprzedni"] > 0 else ("nowe" if r["bieżący"] > 0 else "–"),
        axis=1,
    )
    return merged.sort_values("bieżący", ascending=False)


# ─────────────────────────────────────────
# PAGE CONFIG + STYLES
# ─────────────────────────────────────────

st.set_page_config(page_title="BQ Raport", page_icon="📊", layout="wide")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; background: #0e0e0e; color: #e8e8e8; }
h1, h2, h3 { font-weight: 800; }
.stApp { background: #0e0e0e; }
div[data-testid="stSidebar"] { background: #111; border-right: 1px solid #222; }
.cost-box {
    background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px;
    padding: 12px 18px; font-size: 0.82rem; color: #aaa; margin-bottom: 8px;
}
.cost-box strong { color: #ffd700; }
.cost-box .label { font-size: 0.62rem; text-transform: uppercase; letter-spacing: 2px; color: #555; }
.period-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 2px; color: #666; margin-bottom: 2px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📊 BQ Raport")
    st.markdown("---")

    TABLE = st.secrets.get("STANY") or st.secrets.get("stany")
    if not TABLE:
        available = list(st.secrets.keys())
        st.error(
            "Brak tabeli w secrets.toml. Dodaj linię:\n\n"
            "`STANY = \"projekt.dataset.tabela\"`\n\n"
            f"Dostępne klucze w secrets: `{available}`"
        )
        st.stop()

    st.caption(f"📋 `{TABLE}`")
    st.markdown("---")

    # ── Sklep – ładowany przy starcie osobnym zapytaniem ────────────────────
    creds_hash = str(id(get_credentials()))
    try:
        shops_list = fetch_shops(creds_hash, TABLE)
    except Exception as e:
        shops_list = []
        st.warning(f"Nie można pobrać listy sklepów: {e}")

    if shops_list:
        selected_shop = st.selectbox("🏪 Sklep", shops_list)
    else:
        selected_shop = None
        st.info("Brak danych o sklepach.")

    st.markdown("---")

    with st.expander("📅 Zakres dat", expanded=True):
        preset = st.radio(
            "Szybki wybór",
            ["Ostatni tydzień", "Ostatnie 14 dni", "Ostatnie 30 dni", "Własny"],
            index=0,
        )
        if preset == "Własny":
            c_start = st.date_input("Od", YESTERDAY - timedelta(6), max_value=YESTERDAY)
            c_end   = st.date_input("Do", YESTERDAY, max_value=YESTERDAY)
        else:
            c_start = c_end = None

    current, prev_week, prev_year = get_periods(preset, c_start, c_end)
    n_days = (current[1] - current[0]).days + 1

    st.caption(
        f"**Bieżący:** {current[0]} → {current[1]} ({n_days} dni)\n\n"
        f"**Poprzedni:** {prev_week[0]} → {prev_week[1]}\n\n"
        f"**Rok wcześniej:** {prev_year[0]} → {prev_year[1]}"
    )
    st.markdown("---")

    # Dry run – trzy osobne zapytania
    est = estimate_cost_all(TABLE, [current, prev_week, prev_year])
    if est["ok"]:
        color = "#2ecc71" if est["cost_usd"] < 0.01 else "#ffd700" if est["cost_usd"] < 0.10 else "#ff9f4d"
        st.markdown(f"""
        <div class="cost-box">
            <div class="label">Szacowany koszt (3 zapytania)</div>
            <strong style="color:{color}">${est['cost_usd']:.6f}</strong>
            &nbsp;·&nbsp; <strong>{est['gb']:.4f} GB</strong>
            <div style="font-size:0.68rem;color:#555;margin-top:4px">dry run · $5/TB · 3 osobne query</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning(f"Dry run failed: {est.get('error', '?')}")

    st.markdown("")
    fetch_btn = st.button("🚀 Pobierz dane", use_container_width=True, type="primary")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

st.markdown("# 📊 BQ Raport")
st.markdown(f"`{TABLE}` &nbsp;·&nbsp; bieżący okres: `{current[0]}` → `{current[1]}`")
st.markdown("---")

if "df_cur" not in st.session_state:
    st.session_state.df_cur  = None
    st.session_state.df_prev = None
    st.session_state.df_year = None

if fetch_btn:
    creds_hash_fetch = str(id(get_credentials()))
    try:
        with st.spinner("Pobieranie bieżącego okresu…"):
            st.session_state.df_cur = fetch_period(creds_hash_fetch, TABLE, *current)
        with st.spinner("Pobieranie poprzedniego okresu…"):
            st.session_state.df_prev = fetch_period(creds_hash_fetch, TABLE, *prev_week)
        with st.spinner("Pobieranie roku wcześniej…"):
            st.session_state.df_year = fetch_period(creds_hash_fetch, TABLE, *prev_year)

        total = len(st.session_state.df_cur) + len(st.session_state.df_prev) + len(st.session_state.df_year)
        st.sidebar.success(f"✅ {total:,} wierszy łącznie")
        st.rerun()
    except Exception as e:
        st.error(f"❌ Błąd: {e}")
        st.stop()

df_cur  = st.session_state.df_cur
df_prev = st.session_state.df_prev
df_year = st.session_state.df_year

if df_cur is None:
    st.info("Kliknij **Pobierz dane** w panelu bocznym, aby załadować raport.")
    st.stop()

if selected_shop is None:
    st.warning("Wybierz sklep w panelu bocznym.")
    st.stop()

# ── Filtrowanie po sklepie ───────────────────────────────────────────────────
df_cur_s  = df_cur[df_cur[SHOP_COL]   == selected_shop]
df_prev_s = df_prev[df_prev[SHOP_COL] == selected_shop]
df_year_s = df_year[df_year[SHOP_COL] == selected_shop]

# ── Metryki główne ───────────────────────────────────────────────────────────
st.markdown("### 📦 Liczba produktów")

c1, c2, c3 = st.columns(3)

def delta_str(cur_val, prev_val):
    if prev_val == 0:
        return None
    d   = cur_val - prev_val
    pct = d / prev_val * 100
    return f"{d:+,} ({pct:+.1f}%)"

n_cur  = count_products(df_cur_s)
n_prev = count_products(df_prev_s)
n_year = count_products(df_year_s)

with c1:
    st.markdown('<div class="period-label">Bieżący okres</div>', unsafe_allow_html=True)
    st.metric(f"{current[0]} → {current[1]}", f"{n_cur:,}", delta=delta_str(n_cur, n_prev))
with c2:
    st.markdown('<div class="period-label">Poprzedni okres</div>', unsafe_allow_html=True)
    st.metric(f"{prev_week[0]} → {prev_week[1]}", f"{n_prev:,}")
with c3:
    st.markdown('<div class="period-label">Rok wcześniej</div>', unsafe_allow_html=True)
    st.metric(f"{prev_year[0]} → {prev_year[1]}", f"{n_year:,}", delta=delta_str(n_cur, n_year))

st.markdown("---")

# ── Filtr kategorii ──────────────────────────────────────────────────────────
st.markdown("### 🔍 Analiza kategorii")

available_cats = [c for c in CATEGORY_COLS if c in df_cur.columns]
if not available_cats:
    st.warning("Brak kolumn kategorii w danych.")
    st.stop()

group_col = st.selectbox("Kategoria", available_cats)

tab1, tab2 = st.tabs(["📊 Bieżący vs poprzedni tydzień", "📅 Bieżący vs rok wcześniej"])

def styled_df(cmp):
    return cmp.style.apply(
        lambda col: [
            "color: #2ecc71" if str(v).startswith("+") else
            "color: #e74c3c" if str(v).startswith("-") else ""
            for v in col
        ] if col.name == "zmiana %" else [""] * len(col),
        axis=0,
    )

with tab1:
    cmp = compare_periods(df_cur_s, df_prev_s, group_col)
    st.markdown(f"**{selected_shop}** · `{group_col}` · {current[0]}→{current[1]} vs {prev_week[0]}→{prev_week[1]}")
    st.dataframe(styled_df(cmp), use_container_width=True, height=420)

with tab2:
    cmp_yr = compare_periods(df_cur_s, df_year_s, group_col)
    st.markdown(f"**{selected_shop}** · `{group_col}` · {current[0]}→{current[1]} vs {prev_year[0]}→{prev_year[1]}")
    st.dataframe(styled_df(cmp_yr), use_container_width=True, height=420)

# ── Eksport ──────────────────────────────────────────────────────────────────
st.markdown("---")
csv = df_cur_s.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️ Pobierz bieżący okres CSV",
    data=csv,
    file_name=f"bq_{selected_shop}_{current[0]}_{current[1]}.csv",
    mime="text/csv",
)