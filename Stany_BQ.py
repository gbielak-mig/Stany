"""
BQ Viewer – raport porównawczy (sklep × wszystkie zmienne)
Zoptymalizowane pobieranie równoległe + bezpiecznik + zwijane podsumowanie + mapowanie MPK.
"""

import os, json, pathlib
from datetime import date, timedelta
import concurrent.futures

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# ─────────────────────────────────────────
# KONFIGURACJA I MAPOWANIE MATRIX (MPK)
# ─────────────────────────────────────────

TODAY  = date.today()

CATEGORY_COLS = ["categoryname", "gender", "season", "seasonality", "type"]
DATE_COL      = "event_date"
SHOP_COL      = "shop_name"
INDEX_COL     = "index"
VARIANTS_COL  = "variants"
QUANTITY_COL  = "quantity"

# Słownik mapowania: "Sklep0" (z bazy) -> ("Sklep" [Nazwa przyjazna], "MPK" [Kod])
SHOP_DATA = {
    "Sizeer HR": ("Sizeer HR", "HR50"),
    "Sizeer BG": ("Sizeer BG", "BG50"),
    "50Style PL": ("50Style PL", "S501"),
    "Sizeer LT": ("Sizeer LT", "LT50"),
    "Sizeer HU": ("Sizeer HU", "HU50"),
    "Sizeer SI": ("Sizeer SI", "SI50"),
    "Timberland": ("Timberland", "S502"),
    "Sizeer RO": ("Sizeer RO [new]", "RO50"),
    "Buty Sportowe PL": ("Buty Sportowe", "S514"),
    "Sizeer DE": ("Sizeer DE", "G500"),
    "Sizeer CZ": ("Sizeer CZ", "CZ50"),
    "Symbiosis": ("Symbiosis PL", "S507"),
    "Sizeer LV": ("Sizeer LV", "LV50"),
    "Sizeer SK": ("Sizeer SK", "SK50"),
    "Sizeer PL": ("Sizeer PL [new]", "S500"),
    "Jdsports BG": ("JD BG", "BG52"),
    "Jdsports CZ": ("JD CZ", "CZ55"),
    "Jdsports HU": ("JD HU", "HU52"),
    "Jdsports PL": ("JD PL", "S512"),
    "Jdsports LT": ("JD LT", "LT52"),
    "Jdsports RO": ("JD RO", "RO55"),
    "Jdsports HR": ("JD HR", "HR52"),
    "Jdsports SK": ("JD SK", "SK52"),
    "Jdsports UA": ("JD UA", "UA52")
}


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
# COST ESTIMATE (DRY RUN)
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
# LISTA SKLEPÓW
# ─────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_shops(_creds_hash: str, table: str) -> list:
    project = parse_project(table)
    client  = bigquery.Client(credentials=get_credentials(), project=project)
    query   = f"SELECT DISTINCT {SHOP_COL} FROM `{table}` ORDER BY {SHOP_COL}"
    job     = client.query(query)
    df      = job.result().to_dataframe()
    return sorted(df[SHOP_COL].dropna().tolist())


# ─────────────────────────────────────────
# QUERY
# ─────────────────────────────────────────

def build_query(table: str, start: date, end: date, shop_name: str = None) -> str:
    extra_cols = ", ".join([INDEX_COL] + CATEGORY_COLS + [VARIANTS_COL, QUANTITY_COL])
    shop_filter = f"AND {SHOP_COL} = '{shop_name}'" if shop_name else ""
    
    return f"""
    SELECT {SHOP_COL}, {DATE_COL}, {extra_cols}
    FROM `{table}`
    WHERE {DATE_COL} BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
    {shop_filter}
    """


@st.cache_data(ttl=600, show_spinner=False)
def fetch_period(_creds_hash: str, table: str, start: date, end: date, shop_name: str) -> pd.DataFrame:
    project = parse_project(table)
    client  = bigquery.Client(credentials=get_credentials(), project=project)
    job     = client.query(build_query(table, start, end, shop_name))
    df      = job.result().to_dataframe()
    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL]).dt.date
    return df


# ─────────────────────────────────────────
# ZAKRESY DAT
# ─────────────────────────────────────────

def get_auto_periods(preset: str, custom_start: date = None, custom_end: date = None):
    if preset == "Ostatni tydzień":
        end   = TODAY
        start = end - timedelta(6)
    elif preset == "Ostatnie 14 dni":
        end   = TODAY
        start = end - timedelta(13)
    elif preset == "Ostatnie 30 dni":
        end   = TODAY
        start = end - timedelta(29)
    else:
        start = custom_start or (TODAY - timedelta(6))
        end   = custom_end   or TODAY

    n = (end - start).days + 1
    prev_week = (start - timedelta(n), end - timedelta(n))
    prev_year = (
        start.replace(year=start.year - 1),
        end.replace(year=end.year - 1),
    )
    return (start, end), prev_week, prev_year


# ─────────────────────────────────────────
# AGREGACJA
# ─────────────────────────────────────────

def count_products(df: pd.DataFrame) -> int:
    if INDEX_COL in df.columns:
        return df[INDEX_COL].nunique()
    return len(df)


def sum_col(df: pd.DataFrame, col: str) -> int:
    if col in df.columns:
        return int(df[col].sum())
    return 0


def build_summary_all(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    if not group_cols:
        return pd.DataFrame()
    
    if INDEX_COL in df.columns:
        out = (
            df.groupby(group_cols)[INDEX_COL]
            .nunique()
            .reset_index()
            .rename(columns={INDEX_COL: "produkty"})
        )
    else:
        out = df.groupby(group_cols).size().reset_index(name="produkty")
    return out.sort_values("produkty", ascending=False)


def build_variants_summary_all(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    if not group_cols:
        return pd.DataFrame()
    
    agg = {}
    if VARIANTS_COL in df.columns:
        agg[VARIANTS_COL] = "sum"
    if QUANTITY_COL in df.columns:
        agg[QUANTITY_COL] = "sum"
    if not agg:
        return pd.DataFrame()
    out = df.groupby(group_cols).agg(agg).reset_index()
    sort_col = list(agg.keys())[0]
    return out.sort_values(sort_col, ascending=False)


def compare_periods_all(df_cur, df_prev, df_year, group_cols) -> pd.DataFrame:
    cur  = build_summary_all(df_cur,  group_cols).rename(columns={"produkty": "bieżący"})
    prev = build_summary_all(df_prev, group_cols).rename(columns={"produkty": "poprzedni"})
    year = build_summary_all(df_year, group_cols).rename(columns={"produkty": "rok wcześniej"})
    
    merged = cur.merge(prev, on=group_cols, how="outer").fillna(0)
    merged = merged.merge(year, on=group_cols, how="outer").fillna(0)
    
    merged["bieżący"]       = merged["bieżący"].astype(int)
    merged["poprzedni"]     = merged["poprzedni"].astype(int)
    merged["rok wcześniej"] = merged["rok wcześniej"].astype(int)
    merged["zmiana vs poprz."] = merged["bieżący"] - merged["poprzedni"]
    merged["zmiana % vs poprz."] = merged.apply(
        lambda r: f"{r['zmiana vs poprz.']/r['poprzedni']*100:+.1f}%"
        if r["poprzedni"] > 0 else ("nowe" if r["bieżący"] > 0 else "–"),
        axis=1,
    )
    
    return merged.sort_values("bieżący", ascending=False)


def compare_variants_periods_all(df_cur, df_prev, df_year, group_cols) -> pd.DataFrame:
    cur  = build_variants_summary_all(df_cur,  group_cols)
    prev = build_variants_summary_all(df_prev, group_cols)
    year = build_variants_summary_all(df_year, group_cols)

    if cur.empty:
        return cur

    merged = cur.merge(prev, on=group_cols, how="outer", suffixes=("_cur", "_prev")).fillna(0)
    merged = merged.merge(year, on=group_cols, how="outer", suffixes=("", "_year")).fillna(0)

    result = merged[group_cols].copy()

    for col in [VARIANTS_COL, QUANTITY_COL]:
        col_cur  = f"{col}_cur"
        col_prev = f"{col}_prev"
        col_year = f"{col}_year"
        
        if col_cur in merged.columns:
            merged[col_cur]  = merged[col_cur].astype(int)
            merged[col_prev] = merged[col_prev].astype(int)
            merged[col_year] = merged[col_year].astype(int) if col_year in merged.columns else 0
            
            result[f"{col} (bież.)"]  = merged[col_cur]
            result[f"{col} (poprz.)"] = merged[col_prev]
            result[f"{col} (rok temu)"] = merged[col_year]
            result[f"{col} zmiana vs poprz."]   = merged[col_cur] - merged[col_prev]
            result[f"{col} zmiana % vs poprz."] = merged.apply(
                lambda r: f"{(r[col_cur]-r[col_prev])/r[col_prev]*100:+.1f}%"
                if r[col_prev] > 0 else ("nowe" if r[col_cur] > 0 else "–"),
                axis=1,
            )

    sort_col = f"{VARIANTS_COL} (bież.)" if f"{VARIANTS_COL} (bież.)" in result.columns else result.columns[-1]
    return result.sort_values(sort_col, ascending=False)


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
.filter-tag {
    display: inline-block; background: #1e2a1e; border: 1px solid #2ecc71;
    border-radius: 4px; padding: 2px 8px; font-size: 0.72rem; color: #2ecc71;
    margin: 2px 3px 2px 0;
}
.gross-box { font-size: 0.85rem; margin-top: -8px; margin-bottom: 12px; font-weight: 600; }
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
            "Dostępne klucze w secrets: `{available}`"
        )
        st.stop()

    st.caption(f"📋 `{TABLE}`")
    st.markdown("---")

    # ── Sklep (Z MAPOWANIEM NA MPK) ──────────────────────────────────────────
    creds_hash = str(id(get_credentials()))
    try:
        shops_list = fetch_shops(creds_hash, TABLE)
    except Exception as e:
        shops_list = []
        st.warning(f"Nie można pobrać listy sklepów: {e}")

    if shops_list:
        # Funkcja formatująca wyświetlanie elementów listy na podstawie słownika SHOP_DATA
        def shop_formatter(raw_name):
            if raw_name in SHOP_DATA:
                friendly_name, mpk_code = SHOP_DATA[raw_name]
                return f"{mpk_code} — {friendly_name}"
            return raw_name

        selected_shop = st.selectbox("🏪 Wybierz Sklep (MPK)", shops_list, format_func=shop_formatter)
    else:
        selected_shop = None
        st.info("Brak danych o sklepach.")

    st.markdown("---")

    # ── Zakresy dat ─────────────────────────────────────────────────────────
    with st.expander("📅 Zakres dat", expanded=True):
        preset = st.radio(
            "Szybki wybór",
            ["Ostatni tydzień", "Ostatnie 14 dni", "Ostatnie 30 dni", "Własny"],
            index=0,
        )
        if preset == "Własny":
            c_start = st.date_input("Od", TODAY - timedelta(6), max_value=TODAY)
            c_end   = st.date_input("Do", TODAY, max_value=TODAY)
        else:
            c_start = c_end = None

    auto_current, auto_prev_week, auto_prev_year = get_auto_periods(preset, c_start, c_end)

    with st.expander("🔁 Nadpisz: poprzedni okres"):
        override_prev = st.checkbox("Ustaw ręcznie", key="override_prev")
        if override_prev:
            prev_start = st.date_input("Od##prev", auto_prev_week[0], key="prev_start")
            prev_end   = st.date_input("Do##prev", auto_prev_week[1], key="prev_end")
            prev_week  = (prev_start, prev_end)
        else:
            prev_week = auto_prev_week

    with st.expander("📆 Nadpisz: rok wcześniej"):
        override_year = st.checkbox("Ustaw ręcznie", key="override_year")
        if override_year:
            year_start = st.date_input("Od##year", auto_prev_year[0], key="year_start")
            year_end   = st.date_input("Do##year", auto_prev_year[1], key="year_end")
            prev_year  = (year_start, year_end)
        else:
            prev_year = auto_prev_year

    current = auto_current
    n_days  = (current[1] - current[0]).days + 1

    st.caption(
        f"**Bieżący:** {current[0]} → {current[1]} ({n_days} dni)\n\n"
        f"**Poprzedni:** {prev_week[0]} → {prev_week[1]}"
        + (" ✏️" if override_prev else "") + "\n\n"
        f"**Rok wcześniej:** {prev_year[0]} → {prev_year[1]}"
        + (" ✏️" if override_year else "")
    )
    st.markdown("---")

    # ── Dry run ─────────────────────────────────────────────────────────────
    est = estimate_cost_all(TABLE, [current, prev_week, prev_year])
    if est["ok"]:
        color = "#2ecc71" if est["cost_usd"] < 0.01 else "#ffd700" if est["cost_usd"] < 0.10 else "#ff9f4d"
        st.markdown(f"""
        <div class="cost-box">
            <div class="label">Szacowany koszt (3 zapytania)</div>
            <strong style="color:{color}">${est['cost_usd']:.6f}</strong>
            &nbsp;·&nbsp; <strong>{est['gb']:.4f} GB</strong>
            <div style="font-size:0.68rem;color:#555;margin-top:4px">dry run · $5/TB · zoptymalizowane pod sklep</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning(f"Dry run failed: {est.get('error', '?')}")

    st.markdown("")
    fetch_btn = st.button("🚀 Pobierz dane", use_container_width=True, type="primary")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

# Wyznaczenie czytelnej nazwy sklepu (MPK — Nazwa) do nagłówków
shop_display = f"{SHOP_DATA[selected_shop][1]} — {SHOP_DATA[selected_shop][0]}" if selected_shop in SHOP_DATA else selected_shop

st.markdown("# 📊 BQ Raport")
st.markdown(f"`{TABLE}` &nbsp;·&nbsp; sklep: `{shop_display}` &nbsp;·&nbsp; bieżący okres: `{current[0]}` → `{current[1]}`")
st.markdown("---")

if "df_cur" not in st.session_state:
    st.session_state.df_cur  = None
    st.session_state.df_prev = None
    st.session_state.df_year = None

# POBIERANIE RÓWNOLEGŁE Z FILTREM SQL
if fetch_btn:
    if not selected_shop:
        st.error("Wybierz najpierw sklep!")
        st.stop()

    creds_hash_fetch = str(id(get_credentials()))
    try:
        with st.spinner(f"Pobieranie danych z BigQuery dla {shop_display}…"):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_cur = executor.submit(fetch_period, creds_hash_fetch, TABLE, current[0], current[1], selected_shop)
                future_prev = executor.submit(fetch_period, creds_hash_fetch, TABLE, prev_week[0], prev_week[1], selected_shop)
                future_year = executor.submit(fetch_period, creds_hash_fetch, TABLE, prev_year[0], prev_year[1], selected_shop)
                
                st.session_state.df_cur = future_cur.result()
                st.session_state.df_prev = future_prev.result()
                st.session_state.df_year = future_year.result()

        total = len(st.session_state.df_cur) + len(st.session_state.df_prev) + len(st.session_state.df_year)
        st.sidebar.success(f"✅ {total:,} wierszy dla {shop_display}")
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

# ── Filtrowanie po sklepie (bezpiecznik dla pamięci podręcznej) ───────────────
df_cur_s  = df_cur[df_cur[SHOP_COL]   == selected_shop].copy()
df_prev_s = df_prev[df_prev[SHOP_COL] == selected_shop].copy()
df_year_s = df_year[df_year[SHOP_COL] == selected_shop].copy()


# ─────────────────────────────────────────
# FILTRY (na górze strony głównej)
# ─────────────────────────────────────────
st.markdown("### 🎛️ Filtry")

ALL_FILTER_COLS = CATEGORY_COLS
filter_cols = [c for c in ALL_FILTER_COLS if c in df_cur_s.columns]

active_filters = {}
if filter_cols:
    n_filter_cols = min(len(filter_cols), 5)
    fcols = st.columns(n_filter_cols)
    for i, fc in enumerate(filter_cols):
        with fcols[i % n_filter_cols]:
            unique_vals = sorted(df_cur_s[fc].dropna().unique().tolist())
            selected = st.multiselect(
                f"{fc}",
                options=unique_vals,
                default=[],
                key=f"filter_{fc}",
            )
            if selected:
                active_filters[fc] = selected


# ── Zastosuj filtry ──────────────────────────────────────────────────────────
def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    for col, vals in filters.items():
        if col in df.columns and vals:
            df = df[df[col].isin(vals)]
    return df

df_cur_f  = apply_filters(df_cur_s.copy(),  active_filters)
df_prev_f = apply_filters(df_prev_s.copy(), active_filters)
df_year_f = apply_filters(df_year_s.copy(), active_filters)

if active_filters:
    tags_html = "".join(
        f'<span class="filter-tag">{col}: {val}</span>' for col, val in active_filters.items()
    )
    st.markdown(f"**Aktywne filtry:** {tags_html}", unsafe_allow_html=True)

st.markdown("---")


# ─────────────────────────────────────────
# OBLICZENIA: DODANE / ODJĘTE (Gross Changes)
# ─────────────────────────────────────────

# 1. Zmiany dla unikalnych Produktów (ID)
set_cur = set(df_cur_f[INDEX_COL].dropna().unique()) if INDEX_COL in df_cur_f.columns else set()
set_prev = set(df_prev_f[INDEX_COL].dropna().unique()) if INDEX_COL in df_prev_f.columns else set()

p_added = len(set_cur - set_prev)
p_removed = len(set_prev - set_cur)

# 2. Helper dla wolumenów (Variants i Quantity) rozbitych na plusy i minusy per produkt
def get_gross_changes(df_c, df_p, col):
    if col not in df_c.columns or INDEX_COL not in df_c.columns:
        return 0, 0
    c_sum = df_c.groupby(INDEX_COL)[col].sum()
    p_sum = df_p.groupby(INDEX_COL)[col].sum()
    
    merged = pd.concat([c_sum, p_sum], axis=1, keys=['cur', 'prev']).fillna(0)
    diff = merged['cur'] - merged['prev']
    
    added = int(diff[diff > 0].sum())
    removed = int(abs(diff[diff < 0].sum()))
    return added, removed

v_added, v_removed = get_gross_changes(df_cur_f, df_prev_f, VARIANTS_COL)
q_added, q_removed = get_gross_changes(df_cur_f, df_prev_f, QUANTITY_COL)


# ─────────────────────────────────────────
# ZWIJANE PODSUMOWANIE OKRESU (DEFAULT: UKRYTE)
# ─────────────────────────────────────────

def delta_str(cur_val, prev_val):
    if prev_val == 0:
        return None
    d   = cur_val - prev_val
    pct = d / prev_val * 100
    return f"{d:+,} ({pct:+.1f}%)"

n_cur  = count_products(df_cur_f)
n_prev = count_products(df_prev_f)
n_year = count_products(df_year_f)

v_cur  = sum_col(df_cur_f,  VARIANTS_COL)
v_prev = sum_col(df_prev_f, VARIANTS_COL)
v_year = sum_col(df_year_f, VARIANTS_COL)

q_cur  = sum_col(df_cur_f,  QUANTITY_COL)
q_prev = sum_col(df_prev_f, QUANTITY_COL)
q_year = sum_col(df_year_f, QUANTITY_COL)

# Sekcja ujęta w expander domyślnie zamknięty (expanded=False)
with st.expander("📦 Podsumowanie okresu (kliknij, aby rozwinąć)", expanded=False):
    st.markdown('<div class="period-label">Bieżący okres vs poprzedni</div>', unsafe_allow_html=True)
    r1c1, r1c2, r1c3 = st.columns(3)
    with r1c1:
        st.metric(f"📦 Produkty · {current[0]} → {current[1]}", f"{n_cur:,}", delta=delta_str(n_cur, n_prev))
        st.markdown(f'<div class="gross-box"><span style="color:#2ecc71">▲ +{p_added:,}</span> &nbsp;&nbsp;&nbsp; <span style="color:#e74c3c">▼ -{p_removed:,}</span></div>', unsafe_allow_html=True)
    with r1c2:
        st.metric("🔢 Variants", f"{v_cur:,}", delta=delta_str(v_cur, v_prev))
        st.markdown(f'<div class="gross-box"><span style="color:#2ecc71">▲ +{v_added:,}</span> &nbsp;&nbsp;&nbsp; <span style="color:#e74c3c">▼ -{v_removed:,}</span></div>', unsafe_allow_html=True)
    with r1c3:
        st.metric("📊 Quantity", f"{q_cur:,}", delta=delta_str(q_cur, q_prev))
        st.markdown(f'<div class="gross-box"><span style="color:#2ecc71">▲ +{q_added:,}</span> &nbsp;&nbsp;&nbsp; <span style="color:#e74c3c">▼ -{q_removed:,}</span></div>', unsafe_allow_html=True)

    st.markdown('<div class="period-label" style="margin-top:14px">Poprzedni okres</div>', unsafe_allow_html=True)
    r2c1, r2c2, r2c3 = st.columns(3)
    with r2c1:
        st.metric(f"📦 Produkty · {prev_week[0]} → {prev_week[1]}", f"{n_prev:,}")
    with r2c2:
        st.metric("🔢 Variants", f"{v_prev:,}")
    with r2c3:
        st.metric("📊 Quantity", f"{q_prev:,}")

    st.markdown('<div class="period-label" style="margin-top:14px">Rok wcześniej</div>', unsafe_allow_html=True)
    r3c1, r3c2, r3c3 = st.columns(3)
    with r3c1:
        st.metric(f"📦 Produkty · {prev_year[0]} → {prev_year[1]}", f"{n_year:,}", delta=delta_str(n_cur, n_year))
    with r3c2:
        st.metric("🔢 Variants", f"{v_year:,}", delta=delta_str(v_cur, v_year))
    with r3c3:
        st.metric("📊 Quantity", f"{q_year:,}", delta=delta_str(q_cur, q_year))

st.markdown("---")


# ─────────────────────────────────────────
# AGREGACJA PO WSZYSTKICH ZMIENNYCH FILTRÓW
# ─────────────────────────────────────────

group_cols_all = [c for c in CATEGORY_COLS if c in df_cur_f.columns]


# ─────────────────────────────────────────
# TABS: Produkty / Variants / Quantity
# ─────────────────────────────────────────

book_tab1, book_tab2, book_tab3 = st.tabs(["📦 Produkty", "🔢 Variants", "📊 Quantity"])

def styled_df(cmp):
    pct_cols = [c for c in cmp.columns if "zmiana %" in c]
    def color_col(col):
        if col.name in pct_cols:
            return [
                "color: #2ecc71" if str(v).startswith("+") else
                "color: #e74c3c" if str(v).startswith("-") else ""
                for v in col
            ]
        return [""] * len(col)
    return cmp.style.apply(color_col, axis=0)

# ── TAB 1: Produkty ──────────────────────────────────────────────────────────
with book_tab1:
    cmp = compare_periods_all(df_cur_f, df_prev_f, df_year_f, group_cols_all)
    st.markdown(
        f"**{shop_display}** · "
        f"{current[0]}→{current[1]} vs {prev_week[0]}→{prev_week[1]} vs {prev_year[0]}→{prev_year[1]}"
    )
    st.dataframe(styled_df(cmp), use_container_width=True, height=500)

# ── TAB 2: Variants ──────────────────────────────────────────────────────────
with book_tab2:
    if VARIANTS_COL not in df_cur_s.columns:
        st.warning(f"Brak kolumny `{VARIANTS_COL}` w danych.")
    else:
        cmp_var = compare_variants_periods_all(df_cur_f, df_prev_f, df_year_f, group_cols_all)
        st.markdown(
            f"**{shop_display}** · "
            f"{current[0]}→{current[1]} vs {prev_week[0]}→{prev_week[1]} vs {prev_year[0]}→{prev_year[1]}"
        )
        if cmp_var.empty:
            st.info("Brak danych.")
        else:
            var_cols = [c for c in cmp_var.columns if "variants" in c.lower()]
            display_cols = group_cols_all + var_cols
            display_cols = [c for c in display_cols if c in cmp_var.columns]
            st.dataframe(styled_df(cmp_var[display_cols]), use_container_width=True, height=500)

# ── TAB 3: Quantity ──────────────────────────────────────────────────────────
with book_tab3:
    if QUANTITY_COL not in df_cur_s.columns:
        st.warning(f"Brak kolumny `{QUANTITY_COL}` w danych.")
    else:
        cmp_qty = compare_variants_periods_all(df_cur_f, df_prev_f, df_year_f, group_cols_all)
        st.markdown(
            f"**{shop_display}** · "
            f"{current[0]}→{current[1]} vs {prev_week[0]}→{prev_week[1]} vs {prev_year[0]}→{prev_year[1]}"
        )
        if cmp_qty.empty:
            st.info("Brak danych.")
        else:
            qty_cols = [c for c in cmp_qty.columns if "quantity" in c.lower()]
            display_cols = group_cols_all + qty_cols
            display_cols = [c for c in display_cols if c in cmp_qty.columns]
            st.dataframe(styled_df(cmp_qty[display_cols]), use_container_width=True, height=500)

# ── Eksport ──────────────────────────────────────────────────────────────────
st.markdown("---")
csv = df_cur_f.to_csv(index=False).encode("utf-8")
shop_file_suffix = SHOP_DATA[selected_shop][1] if selected_shop in SHOP_DATA else selected_shop
st.download_button(
    "⬇️ Pobierz bieżący okres CSV",
    data=csv,
    file_name=f"bq_{shop_file_suffix}_{current[0]}_{current[1]}.csv",
    mime="text/csv",
)
