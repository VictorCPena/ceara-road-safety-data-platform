from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import altair as alt
import streamlit as st


APP_NAME = "Observatório Viário CE"
APP_SUBTITLE = "Segurança viária baseada em dados públicos"
DB_PATH = Path(
    os.getenv(
        "DUCKDB_PATH",
        "data/warehouse/ceara_road_safety.duckdb",
    )
)

GEOJSON_PATH = Path(
    os.getenv(
        "CEARA_GEOJSON_PATH",
        "dashboard/assets/ceara_municipalities.geojson",
    )
)

st.set_page_config(
    page_title=APP_NAME,
    page_icon="◼",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
:root {
    --bg: #f5f7fb;
    --surface: #ffffff;
    --surface-2: #f9fafb;
    --text: #0f172a;
    --muted: #64748b;
    --line: #e5e7eb;
    --navy: #0b1324;
    --navy-2: #111c33;
    --accent: #f97316;
    --accent-soft: #fff1e8;
    --success: #16a34a;
    --danger: #dc2626;
}

html, body, [class*="css"] {
    font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont,
                 "Segoe UI", sans-serif;
}

.stApp {
    background: var(--bg);
}

.block-container {
    max-width: 1500px;
    padding-top: 1.2rem;
    padding-bottom: 3rem;
}

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--navy), var(--navy-2));
    border-right: 0;
}

[data-testid="stSidebar"] * {
    color: #f8fafc;
}

[data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] > div,
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
    background: rgba(255,255,255,.08);
    border-color: rgba(255,255,255,.18);
}

[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,.12);
}

[data-testid="stMetric"] {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 16px;
    padding: 1rem 1rem .9rem 1rem;
    box-shadow: 0 8px 28px rgba(15, 23, 42, 0.04);
}

[data-testid="stMetricLabel"] {
    color: var(--muted);
}

[data-testid="stMetricValue"] {
    color: var(--text);
}

.platform-topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    margin-bottom: .6rem;
}

.brand-lockup {
    display: flex;
    align-items: center;
    gap: 12px;
}

.brand-mark {
    width: 42px;
    height: 42px;
    border-radius: 12px;
    background: linear-gradient(135deg, #f97316, #fb923c);
    display: grid;
    place-items: center;
    color: white;
    font-weight: 800;
    letter-spacing: -.04em;
    box-shadow: 0 8px 24px rgba(249, 115, 22, .24);
}

.brand-name {
    font-size: 1.12rem;
    font-weight: 800;
    color: var(--text);
    line-height: 1.1;
}

.brand-sub {
    color: var(--muted);
    font-size: .82rem;
    margin-top: 2px;
}

.status-pill {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    border: 1px solid #dbe4ef;
    background: #fff;
    border-radius: 999px;
    padding: 8px 12px;
    font-size: .80rem;
    color: #334155;
}

.status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #22c55e;
    box-shadow: 0 0 0 4px rgba(34, 197, 94, .12);
}

.hero {
    background:
        radial-gradient(circle at 88% 18%, rgba(249,115,22,.16), transparent 22%),
        linear-gradient(135deg, #0b1324 0%, #13213d 100%);
    color: white;
    border-radius: 22px;
    padding: 1.65rem 1.8rem;
    margin: .8rem 0 1rem 0;
    box-shadow: 0 16px 42px rgba(2, 6, 23, .16);
}

.hero-kicker {
    color: #fdba74;
    font-size: .76rem;
    font-weight: 800;
    letter-spacing: .12em;
    text-transform: uppercase;
    margin-bottom: .45rem;
}

.hero h1 {
    margin: 0;
    font-size: 2.2rem;
    letter-spacing: -.035em;
    line-height: 1.05;
}

.hero p {
    max-width: 860px;
    margin: .65rem 0 0 0;
    color: #cbd5e1;
    line-height: 1.55;
}

.section-title {
    font-size: 1.05rem;
    font-weight: 800;
    color: var(--text);
    margin-top: .25rem;
    margin-bottom: .5rem;
}

.section-subtitle {
    color: var(--muted);
    font-size: .86rem;
    margin-top: -.35rem;
    margin-bottom: .65rem;
}

.card {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 1rem 1.1rem;
    box-shadow: 0 8px 26px rgba(15, 23, 42, 0.04);
}

.insight-card {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 16px;
    padding: .95rem 1rem;
    margin-bottom: .75rem;
}

.insight-label {
    font-size: .72rem;
    text-transform: uppercase;
    letter-spacing: .08em;
    font-weight: 800;
    color: var(--muted);
}

.insight-value {
    margin-top: .28rem;
    font-weight: 800;
    color: var(--text);
    font-size: 1rem;
}

.insight-detail {
    color: var(--muted);
    margin-top: .18rem;
    font-size: .82rem;
    line-height: 1.4;
}

.note {
    background: #fff7ed;
    border: 1px solid #fed7aa;
    color: #9a3412;
    border-radius: 14px;
    padding: .8rem .95rem;
    font-size: .88rem;
    line-height: 1.45;
}

.small-muted {
    color: var(--muted);
    font-size: .80rem;
}

div[data-testid="stPlotlyChart"] {
    background: white;
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: .25rem;
    box-shadow: 0 8px 26px rgba(15, 23, 42, 0.035);
}

.stDataFrame {
    background: white;
    border-radius: 16px;
}

footer {visibility: hidden;}
#MainMenu {visibility: hidden;}
header[data-testid="stHeader"] {
    background: transparent;
}
</style>
""",
    unsafe_allow_html=True,
)





MAP_CLASSIFICATION_COLORS = {
    "Com Vítimas Feridas": "#2563eb",
    "Com Vítimas Fatais": "#ef4444",
    "Sem Vítimas": "#22c55e",
}


@st.cache_data(show_spinner=False)
def load_ceara_geojson() -> dict:
    if not GEOJSON_PATH.exists():
        st.error(
            "GeoJSON municipal do Ceará não encontrado. Execute uma vez:\n\n"
            "`python scripts/download_ceara_geojson.py`"
        )
        st.stop()

    with GEOJSON_PATH.open("r", encoding="utf-8") as file:
        geojson = json.load(file)

    features = geojson.get("features", [])
    if len(features) != 184:
        st.warning(
            f"O GeoJSON contém {len(features)} feições; eram esperados 184 municípios."
        )

    return geojson


def _metric_config(metric: str) -> tuple[str, str, str]:
    return {
        "Acidentes": ("accidents", "Acidentes", "orangered"),
        "Mortes": ("deaths", "Mortes", "reds"),
        "Feridos graves": ("serious_injuries", "Feridos graves", "oranges"),
    }[metric]


def _municipality_geo_features(geojson: dict, municipal_df: pd.DataFrame) -> list[dict]:
    rows = {
        str(row.ibge_code): row
        for row in municipal_df.itertuples(index=False)
    }

    features = []
    for feature in geojson.get("features", []):
        props = dict(feature.get("properties", {}))
        code = str(props.get("ibge_code", ""))
        row = rows.get(code)

        if row is not None:
            props.update({
                "municipality": row.municipality,
                "accidents": int(row.accidents or 0),
                "deaths": int(row.deaths or 0),
                "serious_injuries": int(row.serious_injuries or 0),
                "injured": int(row.injured or 0),
            })
        else:
            props.update({
                "accidents": 0,
                "deaths": 0,
                "serious_injuries": 0,
                "injured": 0,
            })

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": feature.get("geometry"),
        })

    return features


def render_ceara_vector_map(
    municipal_df: pd.DataFrame,
    hotspots_df: pd.DataFrame | None = None,
    accidents_df: pd.DataFrame | None = None,
    *,
    metric: str = "Acidentes",
    height: int = 620,
    key: str,
) -> None:
    geojson = load_ceara_geojson()
    metric_field, metric_title, scheme = _metric_config(metric)
    features = _municipality_geo_features(geojson, municipal_df)

    base = (
        alt.Chart(alt.Data(values=features))
        .mark_geoshape(stroke="#ffffff", strokeWidth=0.65)
        .encode(
            color=alt.Color(
                f"properties.{metric_field}:Q",
                title=metric_title,
                scale=alt.Scale(scheme=scheme),
            ),
            tooltip=[
                alt.Tooltip("properties.municipality:N", title="Município"),
                alt.Tooltip("properties.accidents:Q", title="Acidentes", format=","),
                alt.Tooltip("properties.deaths:Q", title="Mortes", format=","),
                alt.Tooltip("properties.serious_injuries:Q", title="Feridos graves", format=","),
                alt.Tooltip("properties.injured:Q", title="Feridos", format=","),
            ],
        )
    )

    layers = [base]

    if hotspots_df is not None and not hotspots_df.empty:
        h = hotspots_df.dropna(subset=["longitude", "latitude"]).copy()
        if not h.empty:
            layers.append(
                alt.Chart(h)
                .mark_circle(opacity=0.68, stroke="#ffffff", strokeWidth=0.7)
                .encode(
                    longitude=alt.Longitude("longitude:Q"),
                    latitude=alt.Latitude("latitude:Q"),
                    size=alt.Size(
                        "accidents:Q",
                        title="Acidentes no trecho",
                        scale=alt.Scale(range=[35, 520]),
                    ),
                    color=alt.Color(
                        "avg_severity_score:Q",
                        title="Severidade média",
                        scale=alt.Scale(scheme="yelloworangered"),
                    ),
                    tooltip=[
                        alt.Tooltip("highway_segment:N", title="Trecho"),
                        alt.Tooltip("municipality:N", title="Município"),
                        alt.Tooltip("accidents:Q", title="Acidentes", format=","),
                        alt.Tooltip("deaths:Q", title="Mortes", format=","),
                        alt.Tooltip("serious_injuries:Q", title="Feridos graves", format=","),
                        alt.Tooltip("avg_severity_score:Q", title="Severidade média", format=".2f"),
                    ],
                )
            )

    if accidents_df is not None and not accidents_df.empty:
        a = accidents_df.dropna(subset=["longitude", "latitude"]).copy()
        if not a.empty:
            layers.append(
                alt.Chart(a)
                .mark_circle(size=24, opacity=0.50, stroke="#ffffff", strokeWidth=0.35)
                .encode(
                    longitude=alt.Longitude("longitude:Q"),
                    latitude=alt.Latitude("latitude:Q"),
                    color=alt.Color(
                        "accident_classification:N",
                        title="Classificação",
                        scale=alt.Scale(
                            domain=list(MAP_CLASSIFICATION_COLORS.keys()),
                            range=list(MAP_CLASSIFICATION_COLORS.values()),
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("municipality:N", title="Município"),
                        alt.Tooltip("event_timestamp:T", title="Data/hora"),
                        alt.Tooltip("highway:Q", title="BR"),
                        alt.Tooltip("km:Q", title="Km", format=".1f"),
                        alt.Tooltip("accident_classification:N", title="Classificação"),
                        alt.Tooltip("deaths:Q", title="Mortes"),
                        alt.Tooltip("injured:Q", title="Feridos"),
                    ],
                )
            )

    chart = (
        alt.layer(*layers)
        .project(type="mercator")
        .properties(height=height)
        .configure_view(stroke=None)
    )

    st.altair_chart(chart, width="stretch", key=key)


def db_exists() -> None:
    if not DB_PATH.exists():
        st.error(
            f"Warehouse não encontrado em `{DB_PATH}`.\n\n"
            "Execute o pipeline/dbt antes de abrir o observatório."
        )
        st.stop()


def query(sql: str, params: list | tuple | None = None) -> pd.DataFrame:
    db_exists()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql, params or []).df()
    finally:
        con.close()


@st.cache_data(ttl=120, show_spinner=False)
def filter_options():
    years = query(
        """
        select distinct source_year as yr
        from gold.fct_accident
        where source_year is not null
        order by 1
        """
    )["yr"].astype(int).tolist()

    municipalities = query(
        """
        select distinct municipality
        from gold.dim_municipality
        where municipality is not null
        order by 1
        """
    )["municipality"].tolist()

    classifications = query(
        """
        select distinct accident_classification
        from gold.fct_accident
        where accident_classification is not null
        order by 1
        """
    )["accident_classification"].tolist()

    return years, municipalities, classifications


def fmt_int(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{int(v):,}".replace(",", ".")


def fmt_float(v, digits=1) -> str:
    if v is None or pd.isna(v):
        return "—"
    s = f"{float(v):,.{digits}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def accident_filter_sql(years, municipalities, classifications):
    clauses = []
    params = []

    if years:
        clauses.append("fa.source_year in (" + ",".join(["?"] * len(years)) + ")")
        params.extend(years)

    if municipalities:
        clauses.append("dm.municipality in (" + ",".join(["?"] * len(municipalities)) + ")")
        params.extend(municipalities)

    if classifications:
        clauses.append(
            "fa.accident_classification in (" +
            ",".join(["?"] * len(classifications)) + ")"
        )
        params.extend(classifications)

    return (" and ".join(clauses) if clauses else "1=1"), params


def mart_filter_sql(years, municipalities):
    clauses = []
    params = []

    if years:
        clauses.append("m.year in (" + ",".join(["?"] * len(years)) + ")")
        params.extend(years)

    if municipalities:
        clauses.append("m.municipality in (" + ",".join(["?"] * len(municipalities)) + ")")
        params.extend(municipalities)

    return (" and ".join(clauses) if clauses else "1=1"), params



def product_mart_filter_sql(
    alias: str,
    years: list[int],
    municipalities: list[str] | None = None,
) -> tuple[str, list]:
    clauses = []
    params: list = []

    if years:
        clauses.append(
            f"{alias}.year in (" + ",".join(["?"] * len(years)) + ")"
        )
        params.extend(years)

    if municipalities:
        clauses.append(
            f"{alias}.municipality in (" +
            ",".join(["?"] * len(municipalities)) + ")"
        )
        params.extend(municipalities)

    return (" and ".join(clauses) if clauses else "1=1"), params



def map_filter_sql(years, municipalities, classifications):
    clauses = []
    params = []

    if years:
        clauses.append(
            "m.year in (" + ",".join(["?"] * len(years)) + ")"
        )
        params.extend(years)

    if municipalities:
        clauses.append(
            "m.municipality in (" + ",".join(["?"] * len(municipalities)) + ")"
        )
        params.extend(municipalities)

    if classifications:
        clauses.append(
            "m.accident_classification in (" +
            ",".join(["?"] * len(classifications)) + ")"
        )
        params.extend(classifications)

    return (" and ".join(clauses) if clauses else "1=1"), params


def topbar():
    st.markdown(
        f"""
<div class="platform-topbar">
    <div class="brand-lockup">
        <div class="brand-mark">CE</div>
        <div>
            <div class="brand-name">{APP_NAME}</div>
            <div class="brand-sub">{APP_SUBTITLE}</div>
        </div>
    </div>
    <div class="status-pill">
        <span class="status-dot"></span>
        Dados processados pela camada Gold
    </div>
</div>
""",
        unsafe_allow_html=True,
    )


def hero(title, subtitle, kicker="INTELIGÊNCIA VIÁRIA"):
    st.markdown(
        f"""
<div class="hero">
    <div class="hero-kicker">{kicker}</div>
    <h1>{title}</h1>
    <p>{subtitle}</p>
</div>
""",
        unsafe_allow_html=True,
    )


def section(title, subtitle=None):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(
            f'<div class="section-subtitle">{subtitle}</div>',
            unsafe_allow_html=True,
        )


def insight(label, value, detail):
    st.markdown(
        f"""
<div class="insight-card">
    <div class="insight-label">{label}</div>
    <div class="insight-value">{value}</div>
    <div class="insight-detail">{detail}</div>
</div>
""",
        unsafe_allow_html=True,
    )


def polish_fig(fig, height=420, legend_bottom=False):
    fig.update_layout(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=18, r=18, t=42, b=18),
        font=dict(family="Inter, Arial, sans-serif", color="#334155"),
        title_font=dict(size=15, color="#0f172a"),
    )
    fig.update_xaxes(
        gridcolor="#eef2f7",
        zeroline=False,
    )
    fig.update_yaxes(
        gridcolor="#eef2f7",
        zeroline=False,
    )
    if legend_bottom:
        fig.update_layout(
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0,
            )
        )
    return fig


db_exists()
all_years, all_municipalities, all_classifications = filter_options()

st.sidebar.markdown(
    """
<div style="padding:.35rem 0 .7rem 0;">
    <div style="font-size:1.15rem;font-weight:800;">Observatório Viário CE</div>
    <div style="font-size:.78rem;opacity:.68;margin-top:3px;">Inteligência em segurança viária</div>
</div>
""",
    unsafe_allow_html=True,
)

page = st.sidebar.radio(
    "Navegação",
    [
        "Visão geral",
        "Território",
        "Ocorrências",
        "Envolvidos",
        "Dados & metodologia",
    ],
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Filtros globais**")

selected_years = st.sidebar.multiselect(
    "Ano",
    all_years,
    default=all_years,
)

selected_municipalities = st.sidebar.multiselect(
    "Município",
    all_municipalities,
    default=[],
    placeholder="Todos",
)

selected_classifications = st.sidebar.multiselect(
    "Classificação do acidente",
    all_classifications,
    default=[],
    placeholder="Todas",
)

st.sidebar.markdown("---")
st.sidebar.caption(
    "PRF + IBGE + Open-Meteo · Bronze → Silver → Gold"
)

where_sql, where_params = accident_filter_sql(
    selected_years,
    selected_municipalities,
    selected_classifications,
)

mart_where, mart_params = mart_filter_sql(
    selected_years,
    selected_municipalities,
)

map_where, map_params = map_filter_sql(
    selected_years,
    selected_municipalities,
    selected_classifications,
)

topbar()


if page == "Visão geral":
    hero(
        "Onde, quando e com que gravidade os acidentes acontecem no Ceará?",
        "Uma visão integrada de acidentes em rodovias federais, população municipal "
        "e condições meteorológicas. Explore o território, compare municípios e "
        "identifique padrões de severidade.",
    )

    metrics = query(
        f"""
        select
            count(*) as accidents,
            coalesce(sum(fa.deaths), 0) as deaths,
            coalesce(sum(fa.injured), 0) as injured,
            coalesce(sum(fa.serious_injuries), 0) as serious_injuries,
            count(distinct fa.municipality_key) as municipalities
        from gold.fct_accident fa
        join gold.dim_municipality dm
          on dm.municipality_key = fa.municipality_key
        where {where_sql}
        """,
        where_params,
    ).iloc[0]

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Acidentes", fmt_int(metrics["accidents"]))
    m2.metric("Mortes", fmt_int(metrics["deaths"]))
    m3.metric("Feridos", fmt_int(metrics["injured"]))
    m4.metric("Feridos graves", fmt_int(metrics["serious_injuries"]))
    m5.metric("Municípios", fmt_int(metrics["municipalities"]))

    st.write("")

    left, right = st.columns([2.25, .75], gap="large")

    with left:
        section(
            "Mapa operacional",
            "Coroplético municipal com hotspots dos trechos rodoviários mais críticos.",
        )

        home_municipal = query(
            f"""
            select
                m.ibge_code,
                m.municipality,
                sum(m.accidents) as accidents,
                sum(m.deaths) as deaths,
                sum(m.serious_injuries) as serious_injuries,
                sum(m.injured) as injured
            from gold.mart_municipality_road_safety_yearly m
            where {mart_where}
            group by 1,2
            order by accidents desc
            """,
            mart_params,
        )

        home_seg_where, home_seg_params = product_mart_filter_sql(
            "m",
            selected_years,
            selected_municipalities,
        )

        home_segments = query(
            f"""
            select
                m.highway,
                m.highway_segment,
                m.municipality,
                sum(m.accidents) as accidents,
                sum(m.deaths) as deaths,
                sum(m.serious_injuries) as serious_injuries,
                avg(m.avg_severity_score) as avg_severity_score,
                avg(m.center_latitude) as latitude,
                avg(m.center_longitude) as longitude
            from gold.mart_highway_segment_safety m
            where {home_seg_where}
            group by 1,2,3
            having avg(m.center_latitude) is not null
               and avg(m.center_longitude) is not null
            order by accidents desc
            limit 40
            """,
            home_seg_params,
        )

        st.caption(
            "Mapa vetorial do Ceará: municípios coloridos pelo volume de acidentes "
            "e círculos nos trechos rodoviários mais críticos."
        )

        render_ceara_vector_map(
            home_municipal,
            hotspots_df=home_segments,
            metric="Acidentes",
            height=620,
            key="home_ceara_vector_map",
        )

    with right:
        section("Sinais do recorte atual")

        top_mun = query(
            f"""
            select
                dm.municipality,
                count(*) as accidents,
                sum(fa.deaths) as deaths
            from gold.fct_accident fa
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by accidents desc, deaths desc
            limit 1
            """,
            where_params,
        )

        top_cause = query(
            f"""
            select
                bac.accident_cause,
                count(distinct bac.accident_id) as accidents
            from gold.bridge_accident_cause bac
            join gold.fct_accident fa
              on fa.accident_key = bac.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by accidents desc
            limit 1
            """,
            where_params,
        )

        fatal_share = query(
            f"""
            select
                100.0 * avg(case when fa.is_fatal_accident then 1 else 0 end)
                as fatal_pct
            from gold.fct_accident fa
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            """,
            where_params,
        ).iloc[0]["fatal_pct"]

        if not top_mun.empty:
            row = top_mun.iloc[0]
            insight(
                "Maior volume",
                str(row["municipality"]),
                f'{fmt_int(row["accidents"])} acidentes no recorte.',
            )

        if not top_cause.empty:
            row = top_cause.iloc[0]
            insight(
                "Causa mais frequente",
                str(row["accident_cause"]),
                f'{fmt_int(row["accidents"])} acidentes associados.',
            )

        insight(
            "Acidentes fatais",
            f"{fmt_float(fatal_share, 1)}%",
            "Participação de acidentes classificados como fatais.",
        )

        st.markdown(
            """
<div class="note">
2026 é um período parcial. Compare anos considerando a data até a qual cada fonte foi observada.
</div>
""",
            unsafe_allow_html=True,
        )

    st.write("")
    c1, c2 = st.columns([1.15, .85], gap="large")

    with c1:
        section("Evolução anual")
        trend = query(
            f"""
            select
                fa.source_year as yr,
                count(*) as accidents,
                sum(fa.deaths) as deaths,
                sum(fa.injured) as injured
            from gold.fct_accident fa
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by 1
            """,
            where_params,
        )

        if not trend.empty:
            long = trend.melt(
                id_vars=["yr"],
                value_vars=["accidents", "deaths", "injured"],
                var_name="metric",
                value_name="value",
            )
            long["metric"] = long["metric"].map(
                {
                    "accidents": "Acidentes",
                    "deaths": "Mortes",
                    "injured": "Feridos",
                }
            )
            fig = px.line(
                long,
                x="yr",
                y="value",
                color="metric",
                markers=True,
                labels={"yr": "Ano", "value": "Quantidade", "metric": ""},
            )
            polish_fig(fig, 390, legend_bottom=True)
            st.plotly_chart(fig, width="stretch")

    with c2:
        section("Municípios com mais acidentes")
        rank = query(
            f"""
            select
                dm.municipality,
                count(*) as accidents
            from gold.fct_accident fa
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by accidents desc
            limit 10
            """,
            where_params,
        ).sort_values("accidents")

        if not rank.empty:
            fig = px.bar(
                rank,
                x="accidents",
                y="municipality",
                orientation="h",
                labels={"accidents": "Acidentes", "municipality": ""},
            )
            polish_fig(fig, 390)
            st.plotly_chart(fig, width="stretch")

elif page == "Território":
    hero(
        "Território e infraestrutura viária",
        "Onde os acidentes se concentram, quais municípios se destacam e quais trechos rodoviários merecem atenção.",
        "TERRITÓRIO",
    )

    municipal = query(
        f"""
        select
            m.year,
            m.ibge_code,
            m.municipality,
            m.population,
            m.accidents,
            m.fatal_accidents,
            m.deaths,
            m.serious_injuries,
            m.injured,
            m.accidents_per_100k,
            m.deaths_per_100k,
            m.serious_injuries_per_100k,
            m.year_status
        from gold.mart_municipality_road_safety_yearly m
        where {mart_where}
        order by m.year desc, m.accidents desc
        """,
        mart_params,
    )

    territory_tab, municipality_tab, roads_tab = st.tabs(
        ["Mapa", "Municípios", "Rodovias & trechos"]
    )

    with territory_tab:
        metric = st.selectbox(
            "Colorir municípios por",
            ["Acidentes", "Mortes", "Feridos graves"],
            index=0,
            key="territory_metric",
        )

        municipal_map_df = query(
            f"""
            select
                m.ibge_code,
                m.municipality,
                sum(m.accidents) as accidents,
                sum(m.deaths) as deaths,
                sum(m.serious_injuries) as serious_injuries,
                sum(m.injured) as injured
            from gold.mart_municipality_road_safety_yearly m
            where {mart_where}
            group by 1,2
            order by accidents desc
            """,
            mart_params,
        )

        segment_where, segment_params = product_mart_filter_sql(
            "m",
            selected_years,
            selected_municipalities,
        )

        segment_df = query(
            f"""
            select
                m.highway,
                m.highway_segment,
                m.municipality,
                sum(m.accidents) as accidents,
                sum(m.deaths) as deaths,
                sum(m.serious_injuries) as serious_injuries,
                avg(m.avg_severity_score) as avg_severity_score,
                avg(m.center_latitude) as latitude,
                avg(m.center_longitude) as longitude
            from gold.mart_highway_segment_safety m
            where {segment_where}
            group by 1,2,3
            having avg(m.center_latitude) is not null
               and avg(m.center_longitude) is not null
            order by accidents desc
            limit 80
            """,
            segment_params,
        )

        show_individual = st.toggle(
            "Exibir acidentes individuais",
            value=False,
            help=(
                "Desligado por padrão para manter a visualização leve. "
                "Ative para sobrepor os acidentes do recorte."
            ),
            key="territory_show_individual",
        )

        individual_df = None
        if show_individual:
            individual_df = query(
                f"""
                select
                    m.municipality,
                    m.event_timestamp,
                    m.highway,
                    m.km,
                    m.map_latitude as latitude,
                    m.map_longitude as longitude,
                    m.accident_classification,
                    m.deaths,
                    m.injured
                from gold.mart_accident_map m
                where {map_where}
                  and m.coordinate_quality = 'valid'
                order by m.event_timestamp desc
                """,
                map_params,
            )
            st.caption(
                f"{fmt_int(len(individual_df))} acidentes individuais no recorte."
            )

        render_ceara_vector_map(
            municipal_map_df,
            hotspots_df=segment_df,
            accidents_df=individual_df,
            metric=metric,
            height=690,
            key="territory_vector_map",
        )

        st.markdown(
            """
<div class="note">
Municípios são coloridos pela métrica selecionada. Os círculos representam trechos rodoviários agregados em faixas de 10 km. O mapa é exploratório e não mede risco causal.
</div>
""",
            unsafe_allow_html=True,
        )

    with municipality_tab:
        if municipal.empty:
            st.info("Sem dados municipais para os filtros atuais.")
        else:
            municipal["year_status"] = municipal["year_status"].replace(
                {
                    "closed_year": "Ano fechado",
                    "partial_ytd": "Parcial até a data",
                    "future_year": "Ano futuro",
                }
            )

            latest_year = int(municipal["year"].max())
            latest = municipal[municipal["year"] == latest_year].copy()

            a, b = st.columns(2, gap="large")

            with a:
                section(f"População × acidentes · {latest_year}")
                fig = px.scatter(
                    latest,
                    x="population",
                    y="accidents",
                    size=latest["deaths"].fillna(0) + 1,
                    hover_name="municipality",
                    labels={
                        "population": "População",
                        "accidents": "Acidentes",
                    },
                )
                polish_fig(fig, 440)
                st.plotly_chart(fig, width="stretch")

            with b:
                section(f"Taxa de acidentes · {latest_year}")
                rank = (
                    latest.sort_values(
                        "accidents_per_100k",
                        ascending=False,
                    )
                    .head(15)
                    .sort_values("accidents_per_100k")
                )
                fig = px.bar(
                    rank,
                    x="accidents_per_100k",
                    y="municipality",
                    orientation="h",
                    labels={
                        "accidents_per_100k": "Acidentes / 100 mil",
                        "municipality": "",
                    },
                )
                polish_fig(fig, 440)
                st.plotly_chart(fig, width="stretch")

            section("Indicadores municipais")
            st.dataframe(
                municipal[
                    [
                        "year",
                        "municipality",
                        "population",
                        "accidents",
                        "deaths",
                        "serious_injuries",
                        "accidents_per_100k",
                        "deaths_per_100k",
                        "year_status",
                    ]
                ],
                width="stretch",
                hide_index=True,
                column_config={
                    "year": "Ano",
                    "municipality": "Município",
                    "population": st.column_config.NumberColumn("População"),
                    "accidents": st.column_config.NumberColumn("Acidentes"),
                    "deaths": st.column_config.NumberColumn("Mortes"),
                    "serious_injuries": st.column_config.NumberColumn(
                        "Feridos graves"
                    ),
                    "accidents_per_100k": st.column_config.NumberColumn(
                        "Acidentes / 100 mil",
                        format="%.2f",
                    ),
                    "deaths_per_100k": st.column_config.NumberColumn(
                        "Mortes / 100 mil",
                        format="%.2f",
                    ),
                    "year_status": "Status",
                },
            )

    with roads_tab:
        rt_where, rt_params = product_mart_filter_sql(
            "m",
            selected_years,
            selected_municipalities,
        )

        segments = query(
            f"""
            select
                m.highway,
                m.highway_segment,
                m.municipality,
                sum(m.accidents) as accidents,
                sum(m.fatal_accidents) as fatal_accidents,
                sum(m.deaths) as deaths,
                sum(m.serious_injuries) as serious_injuries,
                avg(m.avg_severity_score) as avg_severity_score,
                avg(m.center_latitude) as latitude,
                avg(m.center_longitude) as longitude
            from gold.mart_highway_segment_safety m
            where {rt_where}
            group by 1,2,3
            order by accidents desc
            """,
            rt_params,
        )

        h_where, h_params = product_mart_filter_sql(
            "m",
            selected_years,
            None,
        )

        highways = query(
            f"""
            select
                m.highway,
                sum(m.accidents) as accidents,
                sum(m.deaths) as deaths,
                sum(m.serious_injuries) as serious_injuries,
                avg(m.avg_severity_score) as avg_severity_score
            from gold.mart_highway_safety m
            where {h_where}
            group by 1
            order by accidents desc
            """,
            h_params,
        )

        if selected_classifications:
            st.caption(
                "O filtro de classificação não é aplicado aos marts agregados de rodovia."
            )

        c1, c2 = st.columns(2, gap="large")

        with c1:
            section("BRs com mais acidentes")
            if not highways.empty:
                top_h = highways.head(12).sort_values("accidents")
                top_h["highway_label"] = (
                    "BR-" + top_h["highway"].astype(str)
                )
                fig = px.bar(
                    top_h,
                    x="accidents",
                    y="highway_label",
                    orientation="h",
                    labels={
                        "accidents": "Acidentes",
                        "highway_label": "",
                    },
                )
                polish_fig(fig, 470)
                st.plotly_chart(fig, width="stretch")

        with c2:
            section("Trechos críticos de 10 km")
            if not segments.empty:
                top_s = segments.head(15).sort_values("accidents")
                fig = px.bar(
                    top_s,
                    x="accidents",
                    y="highway_segment",
                    orientation="h",
                    hover_data=[
                        "municipality",
                        "deaths",
                        "serious_injuries",
                    ],
                    labels={
                        "accidents": "Acidentes",
                        "highway_segment": "",
                    },
                )
                polish_fig(fig, 470)
                st.plotly_chart(fig, width="stretch")


elif page == "Ocorrências":
    hero(
        "Dinâmica das ocorrências",
        "Como os acidentes acontecem: causas, tipos, horários e condições meteorológicas associadas.",
        "OCORRÊNCIAS",
    )

    causes_tab, time_tab, climate_tab = st.tabs(
        ["Causas & tipos", "Quando acontece", "Clima"]
    )

    with causes_tab:
        causes = query(
            f"""
            select
                bac.accident_cause,
                count(distinct bac.accident_id) as accidents
            from gold.bridge_accident_cause bac
            join gold.fct_accident fa
              on fa.accident_key = bac.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by accidents desc
            limit 15
            """,
            where_params,
        )

        types = query(
            f"""
            select
                bat.accident_type,
                count(distinct bat.accident_id) as accidents
            from gold.bridge_accident_type bat
            join gold.fct_accident fa
              on fa.accident_key = bat.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by accidents desc
            limit 15
            """,
            where_params,
        )

        c1, c2 = st.columns(2, gap="large")

        with c1:
            section("Principais causas associadas")
            if not causes.empty:
                fig = px.bar(
                    causes.sort_values("accidents"),
                    x="accidents",
                    y="accident_cause",
                    orientation="h",
                    labels={
                        "accidents": "Acidentes",
                        "accident_cause": "",
                    },
                )
                polish_fig(fig, 510)
                st.plotly_chart(fig, width="stretch")

        with c2:
            section("Principais tipos")
            if not types.empty:
                fig = px.bar(
                    types.sort_values("accidents"),
                    x="accidents",
                    y="accident_type",
                    orientation="h",
                    labels={
                        "accidents": "Acidentes",
                        "accident_type": "",
                    },
                )
                polish_fig(fig, 510)
                st.plotly_chart(fig, width="stretch")

        cause_type_where, cause_type_params = product_mart_filter_sql(
            "m",
            selected_years,
            selected_municipalities,
        )

        cause_type = query(
            f"""
            select
                m.accident_cause,
                m.accident_type,
                sum(m.accidents) as accidents
            from gold.mart_accident_cause_type m
            where {cause_type_where}
            group by 1,2
            order by accidents desc
            limit 120
            """,
            cause_type_params,
        )

        section(
            "Causa × tipo de acidente",
            "Cruza as combinações mais frequentes no recorte.",
        )

        if not cause_type.empty:
            top_causes = (
                cause_type.groupby(
                    "accident_cause",
                    as_index=False,
                )["accidents"]
                .sum()
                .nlargest(10, "accidents")["accident_cause"]
                .tolist()
            )
            top_types = (
                cause_type.groupby(
                    "accident_type",
                    as_index=False,
                )["accidents"]
                .sum()
                .nlargest(10, "accidents")["accident_type"]
                .tolist()
            )

            matrix = cause_type[
                cause_type["accident_cause"].isin(top_causes)
                & cause_type["accident_type"].isin(top_types)
            ].pivot_table(
                index="accident_cause",
                columns="accident_type",
                values="accidents",
                aggfunc="sum",
                fill_value=0,
            )

            fig = px.imshow(
                matrix,
                aspect="auto",
                labels={
                    "x": "Tipo de acidente",
                    "y": "Causa",
                    "color": "Acidentes",
                },
            )
            fig.update_layout(
                height=540,
                margin=dict(l=20, r=20, t=25, b=20),
            )
            st.plotly_chart(fig, width="stretch")

        st.markdown(
            """
<div class="note">
Um mesmo acidente pode possuir múltiplas causas ou tipos; essas categorias não são mutuamente exclusivas.
</div>
""",
            unsafe_allow_html=True,
        )

    with time_tab:
        phase = query(
            f"""
            select
                coalesce(
                    fa.day_phase,
                    'Não informado'
                ) as day_phase,
                count(*) as accidents,
                sum(fa.deaths) as deaths
            from gold.fct_accident fa
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by accidents desc
            """,
            where_params,
        )

        section("Fase do dia")
        if not phase.empty:
            fig = px.bar(
                phase,
                x="day_phase",
                y=["accidents", "deaths"],
                barmode="group",
                labels={
                    "day_phase": "Fase do dia",
                    "value": "Quantidade",
                    "variable": "",
                },
            )
            polish_fig(fig, 390, legend_bottom=True)
            st.plotly_chart(fig, width="stretch")

        time_where, time_params = product_mart_filter_sql(
            "m",
            selected_years,
            selected_municipalities,
        )

        temporal = query(
            f"""
            select
                m.day_of_week_number,
                m.day_of_week,
                m.hour,
                sum(m.accidents) as accidents,
                sum(m.fatal_accidents) as fatal_accidents,
                sum(m.deaths) as deaths
            from gold.mart_accident_time_patterns m
            where {time_where}
            group by 1,2,3
            order by 1,3
            """,
            time_params,
        )

        section(
            "Hora × dia da semana",
            "Concentração de acidentes ao longo da semana.",
        )

        if not temporal.empty:
            day_order = [
                "Segunda",
                "Terça",
                "Quarta",
                "Quinta",
                "Sexta",
                "Sábado",
                "Domingo",
            ]
            matrix = temporal.pivot_table(
                index="hour",
                columns="day_of_week",
                values="accidents",
                aggfunc="sum",
                fill_value=0,
            )
            available = [
                day for day in day_order
                if day in matrix.columns
            ]
            matrix = matrix.reindex(columns=available)
            matrix = matrix.reindex(range(24), fill_value=0)

            fig = px.imshow(
                matrix,
                aspect="auto",
                labels={
                    "x": "Dia da semana",
                    "y": "Hora",
                    "color": "Acidentes",
                },
                y=list(range(24)),
            )
            fig.update_layout(
                height=600,
                margin=dict(l=25, r=20, t=20, b=25),
            )
            st.plotly_chart(fig, width="stretch")

    with climate_tab:
        weather_clauses = []
        weather_params = []

        if selected_years:
            weather_clauses.append(
                "year in ("
                + ",".join(["?"] * len(selected_years))
                + ")"
            )
            weather_params.extend(selected_years)

        weather_where = (
            " and ".join(weather_clauses)
            if weather_clauses
            else "1=1"
        )

        weather = query(
            f"""
            select *
            from gold.mart_weather_road_safety_pt
            where {weather_where}
            order by year desc, accidents desc
            """,
            weather_params,
        )

        if selected_municipalities:
            st.caption(
                "O mart meteorológico atual é agregado no nível Ceará × ano × "
                "grupo climático; o filtro municipal ainda não se aplica."
            )

        if weather.empty:
            st.info("Sem dados meteorológicos para os anos selecionados.")
        else:
            groups = (
                weather.groupby(
                    "weather_group_label",
                    as_index=False,
                )
                .agg(
                    accidents=("accidents", "sum"),
                    deaths=("deaths", "sum"),
                    serious_injuries=(
                        "serious_injuries",
                        "sum",
                    ),
                )
                .sort_values(
                    "accidents",
                    ascending=False,
                )
            )

            precip = (
                weather.groupby(
                    "is_precipitating",
                    as_index=False,
                )
                .agg(
                    accidents=("accidents", "sum"),
                    deaths=("deaths", "sum"),
                )
            )
            precip["condition"] = precip[
                "is_precipitating"
            ].map(
                {
                    True: "Com precipitação",
                    False: "Sem precipitação",
                }
            )

            c1, c2 = st.columns(2, gap="large")

            with c1:
                section("Condição meteorológica")
                fig = px.bar(
                    groups.sort_values("accidents"),
                    x="accidents",
                    y="weather_group_label",
                    orientation="h",
                    labels={
                        "accidents": "Acidentes",
                        "weather_group_label": "",
                    },
                )
                polish_fig(fig, 450)
                st.plotly_chart(fig, width="stretch")

            with c2:
                section("Precipitação")
                fig = px.bar(
                    precip,
                    x="condition",
                    y=["accidents", "deaths"],
                    barmode="group",
                    labels={
                        "condition": "",
                        "value": "Quantidade",
                        "variable": "",
                    },
                )
                polish_fig(fig, 450, legend_bottom=True)
                st.plotly_chart(fig, width="stretch")

            st.markdown(
                """
<div class="note">
A análise meteorológica é descritiva. Associação não implica causalidade e deve ser interpretada junto de exposição ao tráfego, local e horário.
</div>
""",
                unsafe_allow_html=True,
            )


elif page == "Envolvidos":
    hero(
        "Pessoas e veículos envolvidos",
        "Quem está envolvido nos acidentes e quais características dos veículos aparecem com maior frequência.",
        "ENVOLVIDOS",
    )

    people_tab, vehicles_tab = st.tabs(
        ["Pessoas", "Veículos"]
    )

    with people_tab:
        sex = query(
            f"""
            select
                coalesce(fp.sex, 'Não informado') as sex,
                count(*) as involvements
            from gold.fct_person_involvement fp
            join gold.fct_accident fa
              on fa.accident_key = fp.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by involvements desc
            """,
            where_params,
        )

        person_type = query(
            f"""
            select
                coalesce(
                    fp.person_type,
                    'Não informado'
                ) as person_type,
                count(*) as involvements
            from gold.fct_person_involvement fp
            join gold.fct_accident fa
              on fa.accident_key = fp.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by involvements desc
            """,
            where_params,
        )

        age = query(
            f"""
            select
                case
                    when fp.age is null then 'Não informado'
                    when fp.age < 18 then '0–17'
                    when fp.age between 18 and 29 then '18–29'
                    when fp.age between 30 and 44 then '30–44'
                    when fp.age between 45 and 59 then '45–59'
                    else '60+'
                end as age_group,
                count(*) as involvements
            from gold.fct_person_involvement fp
            join gold.fct_accident fa
              on fa.accident_key = fp.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            """,
            where_params,
        )

        physical = query(
            f"""
            select
                coalesce(
                    fp.physical_condition,
                    'Não informado'
                ) as physical_condition,
                count(*) as involvements
            from gold.fct_person_involvement fp
            join gold.fct_accident fa
              on fa.accident_key = fp.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            group by 1
            order by involvements desc
            limit 12
            """,
            where_params,
        )

        c1, c2 = st.columns(2, gap="large")

        with c1:
            section("Sexo")
            if not sex.empty:
                fig = px.pie(
                    sex,
                    names="sex",
                    values="involvements",
                    hole=.58,
                )
                polish_fig(fig, 410)
                st.plotly_chart(fig, width="stretch")

        with c2:
            section("Faixa etária")
            if not age.empty:
                order = [
                    "0–17",
                    "18–29",
                    "30–44",
                    "45–59",
                    "60+",
                    "Não informado",
                ]
                age["age_group"] = pd.Categorical(
                    age["age_group"],
                    categories=order,
                    ordered=True,
                )
                age = age.sort_values("age_group")
                fig = px.bar(
                    age,
                    x="age_group",
                    y="involvements",
                    labels={
                        "age_group": "Faixa etária",
                        "involvements": "Envolvimentos",
                    },
                )
                polish_fig(fig, 410)
                st.plotly_chart(fig, width="stretch")

        c3, c4 = st.columns(2, gap="large")

        with c3:
            section("Tipo de envolvido")
            if not person_type.empty:
                fig = px.bar(
                    person_type.sort_values("involvements"),
                    x="involvements",
                    y="person_type",
                    orientation="h",
                    labels={
                        "involvements": "Envolvimentos",
                        "person_type": "",
                    },
                )
                polish_fig(fig, 420)
                st.plotly_chart(fig, width="stretch")

        with c4:
            section("Estado físico")
            if not physical.empty:
                fig = px.bar(
                    physical.sort_values("involvements"),
                    x="involvements",
                    y="physical_condition",
                    orientation="h",
                    labels={
                        "involvements": "Envolvimentos",
                        "physical_condition": "",
                    },
                )
                polish_fig(fig, 420)
                st.plotly_chart(fig, width="stretch")

    with vehicles_tab:
        vehicle_df = query(
            f"""
            select
                fv.source_year as year,
                coalesce(
                    fv.vehicle_type,
                    'Não informado'
                ) as vehicle_type,
                coalesce(
                    fv.vehicle_make,
                    'Não informado'
                ) as vehicle_make,
                coalesce(
                    fv.vehicle_model,
                    'Não informado'
                ) as vehicle_model,
                coalesce(
                    fv.vehicle_make_model,
                    'Não informado'
                ) as vehicle_make_model,
                fv.is_vehicle_make_missing,
                fv.is_vehicle_model_missing,
                fv.vehicle_age,
                fv.occupants,
                fv.deaths,
                fv.serious_injuries,
                fv.minor_injuries,
                fv.uninjured,
                fa.accident_key,
                dm.municipality
            from gold.fct_vehicle_involvement fv
            join gold.fct_accident fa
              on fa.accident_key = fv.accident_key
            join gold.dim_municipality dm
              on dm.municipality_key = fa.municipality_key
            where {where_sql}
            """,
            where_params,
        )

        if vehicle_df.empty:
            st.info("Sem veículos identificáveis para os filtros atuais.")
        else:
            total_vehicles = len(vehicle_df)
            total_accidents = vehicle_df[
                "accident_key"
            ].nunique()
            total_occupants = vehicle_df[
                "occupants"
            ].fillna(0).sum()
            avg_age = vehicle_df[
                "vehicle_age"
            ].dropna().mean()

            a, b, c, d = st.columns(4)
            a.metric(
                "Veículos envolvidos",
                fmt_int(total_vehicles),
            )
            b.metric(
                "Acidentes com veículo",
                fmt_int(total_accidents),
            )
            c.metric(
                "Ocupantes",
                fmt_int(total_occupants),
            )
            d.metric(
                "Idade média",
                f"{fmt_float(avg_age, 1)} anos",
            )

            type_stats = (
                vehicle_df.groupby(
                    "vehicle_type",
                    as_index=False,
                )
                .agg(
                    vehicles=("accident_key", "size"),
                    accidents=(
                        "accident_key",
                        "nunique",
                    ),
                    deaths=("deaths", "sum"),
                    serious_injuries=(
                        "serious_injuries",
                        "sum",
                    ),
                )
                .sort_values(
                    "vehicles",
                    ascending=False,
                )
            )

            c1, c2 = st.columns(2, gap="large")

            with c1:
                section("Veículos por tipo")
                top_types = type_stats.head(15).sort_values(
                    "vehicles"
                )
                fig = px.bar(
                    top_types,
                    x="vehicles",
                    y="vehicle_type",
                    orientation="h",
                    labels={
                        "vehicles": "Veículos",
                        "vehicle_type": "",
                    },
                )
                polish_fig(fig, 470)
                st.plotly_chart(fig, width="stretch")

            with c2:
                section("Severidade por tipo")
                top_severity = type_stats.head(12).copy()
                long = top_severity.melt(
                    id_vars=["vehicle_type"],
                    value_vars=[
                        "deaths",
                        "serious_injuries",
                    ],
                    var_name="metric",
                    value_name="value",
                )
                long["metric"] = long["metric"].map(
                    {
                        "deaths": "Mortes",
                        "serious_injuries": "Feridos graves",
                    }
                )
                fig = px.bar(
                    long,
                    x="vehicle_type",
                    y="value",
                    color="metric",
                    barmode="group",
                    labels={
                        "vehicle_type": "Tipo",
                        "value": "Quantidade",
                        "metric": "",
                    },
                )
                polish_fig(
                    fig,
                    470,
                    legend_bottom=True,
                )
                st.plotly_chart(fig, width="stretch")

            known = vehicle_df[
                ~vehicle_df[
                    "is_vehicle_make_missing"
                ].fillna(True)
            ].copy()

            makes = (
                known.groupby(
                    "vehicle_make",
                    as_index=False,
                )
                .agg(
                    vehicles=("accident_key", "size")
                )
                .nlargest(12, "vehicles")
                .sort_values("vehicles")
            )

            models = (
                known[
                    ~known[
                        "is_vehicle_model_missing"
                    ].fillna(True)
                ]
                .groupby(
                    "vehicle_make_model",
                    as_index=False,
                )
                .agg(
                    vehicles=("accident_key", "size")
                )
                .nlargest(12, "vehicles")
                .sort_values("vehicles")
            )

            c3, c4 = st.columns(2, gap="large")

            with c3:
                section("Fabricantes")
                if not makes.empty:
                    fig = px.bar(
                        makes,
                        x="vehicles",
                        y="vehicle_make",
                        orientation="h",
                        labels={
                            "vehicles": "Veículos",
                            "vehicle_make": "",
                        },
                    )
                    polish_fig(fig, 430)
                    st.plotly_chart(
                        fig,
                        width="stretch",
                    )

            with c4:
                section("Modelos")
                if not models.empty:
                    fig = px.bar(
                        models,
                        x="vehicles",
                        y="vehicle_make_model",
                        orientation="h",
                        labels={
                            "vehicles": "Veículos",
                            "vehicle_make_model": "",
                        },
                    )
                    polish_fig(fig, 430)
                    st.plotly_chart(
                        fig,
                        width="stretch",
                    )

            missing_make = int(
                vehicle_df[
                    "is_vehicle_make_missing"
                ].fillna(True).sum()
            )

            st.caption(
                f"{fmt_int(missing_make)} veículos sem fabricante identificável "
                "foram mantidos na base e excluídos dos rankings."
            )

            st.markdown(
                """
<div class="note">
As métricas descrevem veículos envolvidos nos registros observados. Sem denominador de frota circulante ou quilometragem percorrida, não representam risco relativo entre tipos de veículo.
</div>
""",
                unsafe_allow_html=True,
            )


else:
    hero(
        "Dados, arquitetura e metodologia",
        "Como fontes públicas são transformadas em uma plataforma analítica reproduzível.",
        "PLATAFORMA",
    )

    a, b, c = st.columns(3)

    with a:
        insight(
            "Fontes",
            "PRF + IBGE + Open-Meteo",
            "Acidentes, população municipal e histórico meteorológico.",
        )

    with b:
        insight(
            "Arquitetura",
            "Bronze → Silver → Gold",
            "Rastreabilidade, padronização e consumo analítico.",
        )

    with c:
        insight(
            "Orquestração",
            "Apache Airflow",
            "Dependências, retries e controle de execução.",
        )

    st.write("")
    section("Fluxo da plataforma")
    st.code(
        """
PRF / IBGE / Open-Meteo
          │
          ▼
       Bronze
 snapshots imutáveis
          │
          ▼
       Silver
 dados tipados e limpos
          │
          ▼
   Quality Gates
          │
          ▼
      dbt Gold
          │
          ▼
 Observatório Viário CE
        """,
        language="text",
    )

    section("Cobertura e interpretação")
    st.markdown(
        """
- A PRF cobre **rodovias federais**, não todos os acidentes do Ceará.
- 2026 representa um **período parcial** na base atual.
- Taxas municipais usam população oficial do IBGE.
- Causas e tipos podem ser muitos-para-muitos por acidente.
- Veículos são deduplicados por `accident_key + vehicle_id`.
- Fabricante e modelo são tratados separadamente; ausências permanecem rastreáveis.
- Trechos rodoviários são agrupados em faixas exploratórias de 10 km.
- Clima informado pela PRF e clima modelado pelo Open-Meteo são fontes distintas.
- Associação meteorológica não implica causalidade.
        """
    )

    section("Stack")
    st.markdown(
        "`Python` · `Parquet` · `DuckDB` · `dbt` · `Airflow` · "
        "`PostgreSQL` · `Docker` · `GitHub Actions` · `Streamlit` · "
        "`Plotly` · `Altair/Vega-Lite`"
    )

