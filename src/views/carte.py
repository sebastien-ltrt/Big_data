"""
Carte interactive — Parkings de Rennes
Interface dédiée : disponibilité temps réel avec code couleur vert / jaune / rouge
Lancement : streamlit run src/views/carte.py
"""
import sys
from pathlib import Path

import pytz

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

PARIS_TZ = pytz.timezone("Europe/Paris")

# Groupes couleur (ordre = ordre des traces sur la carte)
COLOR_GROUPS = [
    ("#2ecc71", "🟢 Disponible  < 70 %"),
    ("#f39c12", "🟡 Quasi-plein  70–90 %"),
    ("#e74c3c", "🔴 Complet / Critique  ≥ 90 %"),
    ("#95a5a6", "⚫ Fermé"),
]

st.set_page_config(
    page_title="Carte Parkings Rennes",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=60_000, key="autorefresh_carte")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
.main .block-container { padding-top: 0.8rem; padding-bottom: 0.5rem; }

/* KPI cards */
.kpi-box {
    background: #1a1d27;
    border-radius: 14px;
    padding: 14px 10px;
    text-align: center;
    box-shadow: 0 2px 12px rgba(0,0,0,0.4);
    border-top: 4px solid;
    margin-bottom: 4px;
}
.kpi-number { font-size: 1.9rem; font-weight: 800; line-height: 1.1; }
.kpi-label  { font-size: 0.65rem; color: #8899aa; text-transform: uppercase;
               letter-spacing: 0.07em; margin-top: 3px; }

/* Parking cards sidebar */
.parking-card {
    background: #22263a;
    border-radius: 10px;
    padding: 9px 12px;
    margin-bottom: 6px;
    border-left: 5px solid;
    box-shadow: 0 2px 8px rgba(0,0,0,0.35);
}
.card-name  { font-weight: 700; font-size: 0.82rem; color: #ffffff; }
.card-badge {
    display: inline-block;
    font-size: 0.62rem; font-weight: 700;
    padding: 1px 6px; border-radius: 10px; color: #fff;
    margin-left: 4px; vertical-align: middle;
}
.card-free  { font-size: 1.3rem; font-weight: 800; }
.card-meta  { font-size: 0.68rem; color: #8899aa; margin-top: 2px; }
.bar-bg  { background: #2e3350; border-radius: 4px; height: 5px;
            overflow: hidden; margin-top: 4px; }
.bar-fill { height: 5px; border-radius: 4px; }

/* Legend */
.leg { display:flex; align-items:center; gap:8px; margin:3px 0;
       font-size:0.78rem; color:#ccddee; }
.leg-dot { width:13px; height:13px; border-radius:50%; flex-shrink:0; }

.sec { font-size:0.68rem; font-weight:700; text-transform:uppercase;
       letter-spacing:.08em; color:#6677aa; margin:10px 0 5px; }
</style>
""",
    unsafe_allow_html=True,
)

# ── Chargement ────────────────────────────────────────────────────────────────


@st.cache_data(ttl=60)
def load_parkings() -> pd.DataFrame:
    try:
        from src.models.warehouse import load_parkings_df

        df = load_parkings_df()
        if not df.empty:
            return df
    except Exception:
        pass
    csv = Path("data/processed/latest.csv")
    if csv.exists():
        return pd.read_csv(csv)
    return pd.DataFrame()


def get_status(rate: float, is_full: bool, is_open: bool) -> tuple[str, str, str]:
    if not is_open:
        return "#95a5a6", "Fermé", "gray"
    if is_full or rate >= 90:
        return "#e74c3c", "Complet", "red"
    if rate >= 70:
        return "#f39c12", "Quasi-plein", "yellow"
    return "#2ecc71", "Disponible", "green"


# ── Données ───────────────────────────────────────────────────────────────────

df = load_parkings()

if df.empty:
    st.error("Aucune donnée disponible. Lancez le pipeline : `python -m src.controllers.pipeline`")
    st.stop()

for col in ("free_spaces", "total_spaces", "occupied_spaces"):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
if "occupancy_rate" in df.columns:
    df["occupancy_rate"] = pd.to_numeric(df["occupancy_rate"], errors="coerce").fillna(0.0)
for col in ("is_open", "is_full", "is_critical"):
    df[col] = df[col].fillna(False).astype(bool) if col in df.columns else False

map_df = df.dropna(subset=["lat", "lon"]).copy().reset_index(drop=True)

statuses = map_df.apply(
    lambda r: pd.Series(get_status(
        float(r.get("occupancy_rate", 0)),
        bool(r.get("is_full", False)),
        bool(r.get("is_open", True)),
    )),
    axis=1,
)
map_df["hex_color"] = statuses[0]
map_df["label_statut"] = statuses[1]
map_df["css_class"] = statuses[2]

# Taille markers : proportionnelle à la capacité (28–56 px)
map_df["marker_size"] = (
    map_df["total_spaces"].clip(upper=1200).map(lambda x: max(28, min(56, x / 26 + 18)))
)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🅿️ Parkings Rennes")

    if "temperature_c" in df.columns and pd.notna(df["temperature_c"].iloc[0]):
        temp = df["temperature_c"].iloc[0]
        desc_raw = df["weather_description"].iloc[0] if "weather_description" in df.columns else None
        wind_raw = df["wind_speed_kmh"].iloc[0] if "wind_speed_kmh" in df.columns else None
        desc = str(desc_raw) if desc_raw and str(desc_raw) not in ("None", "nan", "") else "—"
        wind = f"{wind_raw:.0f}" if wind_raw and pd.notna(wind_raw) else "—"
        st.markdown(f"🌤️ {desc} · {temp} °C · 💨 {wind} km/h")

    if "snapshot_time" in df.columns:
        last_ts = pd.to_datetime(df["snapshot_time"], utc=True).max()
        if pd.notna(last_ts):
            st.caption(f"Mis à jour : **{last_ts.astimezone(PARIS_TZ).strftime('%d/%m à %H:%M')}**")

    st.markdown("---")
    st.markdown('<div class="sec">Filtres</div>', unsafe_allow_html=True)
    type_filter = st.multiselect(
        "Type",
        ["Centre-ville", "Parc-Relais"],
        default=["Centre-ville", "Parc-Relais"],
    )
    show_only_open = st.checkbox("Ouverts uniquement", value=True)
    show_only_avail = st.checkbox("Masquer les complets", value=False)

    if st.button("🔄 Rafraîchir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown('<div class="sec">Légende</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="leg"><div class="leg-dot" style="background:#2ecc71"></div>Disponible &lt; 70 %</div>
        <div class="leg"><div class="leg-dot" style="background:#f39c12"></div>Quasi-plein 70–90 %</div>
        <div class="leg"><div class="leg-dot" style="background:#e74c3c"></div>Complet / Critique ≥ 90 %</div>
        <div class="leg"><div class="leg-dot" style="background:#95a5a6"></div>Fermé</div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")
    st.markdown('<div class="sec">Liste des parkings</div>', unsafe_allow_html=True)

    sidebar_df = map_df.copy()
    if type_filter:
        sidebar_df = sidebar_df[sidebar_df["type"].isin(type_filter)]
    if show_only_open:
        sidebar_df = sidebar_df[sidebar_df["is_open"]]
    if show_only_avail:
        sidebar_df = sidebar_df[~sidebar_df["is_full"]]

    for _, row in sidebar_df.sort_values("occupancy_rate", ascending=False).iterrows():
        hex_c = row["hex_color"]
        rate = float(row.get("occupancy_rate", 0))
        free = int(row.get("free_spaces", 0))
        total = int(row.get("total_spaces", 0))
        bar = min(int(rate), 100)
        badge = "CV" if row.get("type") == "Centre-ville" else "P+R"
        label = row["label_statut"]

        st.markdown(
            f"""
            <div class="parking-card" style="border-color:{hex_c}">
              <div style="display:flex;justify-content:space-between;align-items:center">
                <span class="card-name">{row["name"]}</span>
                <span class="card-badge" style="background:{hex_c}">{badge}</span>
              </div>
              <div style="margin:3px 0">
                <span class="card-free" style="color:{hex_c}">{free}</span>
                <span style="font-size:0.78rem;color:#95a5a6"> / {total} places</span>
              </div>
              <div class="bar-bg">
                <div class="bar-fill" style="width:{bar}%;background:{hex_c}"></div>
              </div>
              <div class="card-meta">{label} · {rate:.0f} % occupé</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ── KPIs ──────────────────────────────────────────────────────────────────────

st.markdown("# 🗺️ Carte des parkings — Rennes")

open_count = int(df["is_open"].sum())
total_free = int(df["free_spaces"].sum())
total_cap = int(df["total_spaces"].sum())
critical_count = int(df["is_critical"].sum())
full_count = int(df["is_full"].sum())

c1, c2, c3, c4, c5 = st.columns(5)
kpis = [
    (c1, "#3498db", open_count,              "Parkings ouverts"),
    (c2, "#2ecc71", f"{total_free:,}",       "Places libres"),
    (c3, "#95a5a6", f"{total_cap-total_free:,}", "Places occupées"),
    (c4, "#f39c12", critical_count,          "Quasi-pleins"),
    (c5, "#e74c3c", full_count,              "Complets"),
]
for col, color, val, label in kpis:
    with col:
        st.markdown(
            f"""<div class="kpi-box" style="border-color:{color}">
                <div class="kpi-number" style="color:{color}">{val}</div>
                <div class="kpi-label">{label}</div>
            </div>""",
            unsafe_allow_html=True,
        )

st.markdown("<div style='margin:6px 0'></div>", unsafe_allow_html=True)

# ── Carte ─────────────────────────────────────────────────────────────────────

map_filtered = map_df.copy()
if type_filter:
    map_filtered = map_filtered[map_filtered["type"].isin(type_filter)]
if show_only_open:
    map_filtered = map_filtered[map_filtered["is_open"]]
if show_only_avail:
    map_filtered = map_filtered[~map_filtered["is_full"]]
map_filtered = map_filtered.reset_index(drop=True)

if map_filtered.empty:
    st.info("Aucun parking à afficher avec les filtres sélectionnés.")
else:
    def build_hover(row) -> str:
        rate = float(row.get("occupancy_rate", 0))
        free = int(row.get("free_spaces", 0))
        total = int(row.get("total_spaces", 0))
        occ = int(row.get("occupied_spaces", total - free))
        label = row["label_statut"]
        ptype = row.get("type", "")
        filled = round(rate / 10)
        bar = "█" * filled + "░" * (10 - filled)
        lines = [
            f"<b style='font-size:1.1em'>{row['name']}</b>",
            f"<span style='color:#7f8c8d'>{ptype}</span>",
            "─" * 26,
            f"Statut : <b style='color:{row['hex_color']}'>{label}</b>",
            f"Places libres : <b>{free}</b> / {total}",
            f"Occupation : {bar} {rate:.0f} %",
        ]
        if ptype == "Parc-Relais":
            extras = []
            if float(row.get("total_ev", 0) or 0) > 0:
                extras.append(f"⚡ VE : {int(row.get('free_ev', 0))} / {int(row.get('total_ev', 0))}")
            if float(row.get("total_pmr", 0) or 0) > 0:
                extras.append(f"♿ PMR : {int(row.get('free_pmr', 0))} / {int(row.get('total_pmr', 0))}")
            if float(row.get("total_carpool", 0) or 0) > 0:
                extras.append(f"🚗 Covoiturage : {int(row.get('free_carpool', 0))} / {int(row.get('total_carpool', 0))}")
            if extras:
                lines += ["─" * 26] + extras
        return "<br>".join(lines)

    map_filtered["hover_html"] = map_filtered.apply(build_hover, axis=1)

    # ── Un trace Plotly par groupe couleur ────────────────────────────────────
    # Cela garantit des couleurs discrètes nettes ET une légende native Plotly.
    # mode="markers+text" place le texte centré sur le marqueur (comportement
    # par défaut de Scattermapbox quand textposition n'est pas forcé).
    fig = go.Figure()

    for hex_c, legend_label in COLOR_GROUPS:
        sub = map_filtered[map_filtered["hex_color"] == hex_c].reset_index(drop=True)
        if sub.empty:
            continue

        fig.add_trace(
            go.Scattermapbox(
                lat=sub["lat"],
                lon=sub["lon"],
                mode="markers+text",
                marker=go.scattermapbox.Marker(
                    size=sub["marker_size"],
                    color=hex_c,
                    opacity=0.92,
                ),
                # Chiffre affiché AU CENTRE du cercle (default textposition)
                text=sub["free_spaces"].astype(int).astype(str),
                textfont=dict(size=13, color="white", family="Arial Black, Arial Bold, sans-serif"),
                hovertext=sub["hover_html"],
                hoverinfo="text",
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor=hex_c,
                    font=dict(size=13, color="#2c3e50", family="sans-serif"),
                    align="left",
                ),
                name=legend_label,
            )
        )

    # Noms des parkings affichés juste sous chaque cercle
    fig.add_trace(
        go.Scattermapbox(
            lat=map_filtered["lat"],
            lon=map_filtered["lon"],
            mode="text",
            text=map_filtered["name"],
            textposition="bottom center",
            textfont=dict(size=9, color="#2c3e50", family="Arial, sans-serif"),
            hoverinfo="skip",
            showlegend=False,
        )
    )

    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=48.109, lon=-1.677),
            zoom=11.8,
        ),
        margin=dict(r=0, t=0, l=0, b=0),
        height=620,
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            x=0.01,
            y=0.99,
            bgcolor="rgba(255,255,255,0.92)",
            bordercolor="#ddd",
            borderwidth=1,
            font=dict(size=12, color="#2c3e50"),
        ),
        hoverdistance=25,
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # Ligne de légende / explication sous la carte
    l1, l2, l3, l4 = st.columns([1, 1, 1, 3])
    l1.success("🟢 Disponible < 70 %")
    l2.warning("🟡 Quasi-plein 70–90 %")
    l3.error("🔴 Complet ≥ 90 %")
    l4.caption(
        "La **taille** des cercles est proportionnelle à la capacité totale. "
        "Le **chiffre** au centre indique le nombre de places libres."
    )
