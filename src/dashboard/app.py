"""
Dashboard Streamlit — Parkings Rennes
Disponibilité temps réel : parkings centre-ville (Citedia) + parcs-relais (STAR P+R)
"""
import sys
from pathlib import Path
import pytz

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

PARIS_TZ = pytz.timezone("Europe/Paris")

st.set_page_config(
    page_title="Parkings Rennes",
    page_icon="🅿️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=60_000, key="autorefresh")


# ── Chargement ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_parkings() -> pd.DataFrame:
    try:
        from src.storage.warehouse import load_parkings_df
        df = load_parkings_df()
        if not df.empty:
            return df
    except Exception:
        pass
    csv = Path("data/processed/latest.csv")
    if csv.exists():
        return pd.read_csv(csv)
    return pd.DataFrame()


@st.cache_data(ttl=60)
def load_history() -> pd.DataFrame:
    try:
        from src.storage.warehouse import load_availability_history
        df = load_availability_history(24)
        if not df.empty:
            return df
    except Exception:
        pass
    from src.storage.data_lake import load_parking_history
    return load_parking_history(24)


@st.cache_data(ttl=60)
def load_weather_hist() -> pd.DataFrame:
    try:
        from src.storage.warehouse import load_weather_history
        df = load_weather_history(24)
        if not df.empty:
            return df
    except Exception:
        pass
    from src.storage.data_lake import load_weather_history
    return load_weather_history(24)


def to_paris(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(PARIS_TZ)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🅿️ Parkings Rennes")
    source_filter = st.multiselect(
        "Type de parking",
        ["Centre-ville", "Parc-Relais"],
        default=["Centre-ville", "Parc-Relais"],
    )
    show_full = st.checkbox("Masquer les parkings complets", False)
    search    = st.text_input("Rechercher un parking", "")
    if st.button("Rafraîchir"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Auto-refresh toutes les 60 s.")


# ── Données ───────────────────────────────────────────────────────────────────

df = load_parkings()

if df.empty:
    st.error("Aucune donnée. Lancez `python src/pipeline.py`.")
    st.stop()

if "type" in df.columns and source_filter:
    df = df[df["type"].isin(source_filter)]
if search:
    df = df[df["name"].str.contains(search, case=False, na=False)]
if show_full and "is_full" in df.columns:
    df = df[~df["is_full"]]


# ── KPIs ──────────────────────────────────────────────────────────────────────

st.title("Parkings Rennes — Disponibilité en temps réel")

if "snapshot_time" in df.columns:
    last_ts = pd.to_datetime(df["snapshot_time"], utc=True).max()
    if pd.notna(last_ts):
        st.caption(f"Dernière mise à jour : **{last_ts.astimezone(PARIS_TZ).strftime('%d/%m/%Y %H:%M')}**")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Parkings ouverts",    int(df["is_open"].sum()) if "is_open" in df.columns else len(df))
c2.metric("Places libres",       int(df["free_spaces"].sum()) if "free_spaces" in df.columns else "—")
c3.metric("Places occupées",     int(df["occupied_spaces"].sum()) if "occupied_spaces" in df.columns else "—")
c4.metric("Parkings critiques",  int(df["is_critical"].sum()) if "is_critical" in df.columns else 0)
c5.metric("Parkings complets",   int(df["is_full"].sum()) if "is_full" in df.columns else 0)

st.divider()

tab_map, tab_list, tab_pr, tab_trends, tab_weather, tab_data = st.tabs(
    ["Carte", "Centre-ville", "Parcs-Relais", "Tendances 24h", "Météo", "Données brutes"]
)

# ── Carte ─────────────────────────────────────────────────────────────────────
with tab_map:
    map_df = df.dropna(subset=["lat", "lon"]).copy()
    if not map_df.empty:
        map_df["size"] = map_df["free_spaces"].clip(lower=5)
        fig = px.scatter_mapbox(
            map_df,
            lat="lat", lon="lon",
            size="size",
            color="occupancy_rate",
            color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            hover_name="name",
            hover_data={
                "free_spaces": True,
                "occupied_spaces": True,
                "occupancy_rate": ":.1f",
                "type": True,
                "status": True,
                "size": False, "lat": False, "lon": False,
            },
            symbol="type" if "type" in map_df.columns else None,
            mapbox_style="open-street-map",
            zoom=12,
            center={"lat": 48.109, "lon": -1.677},
            height=560,
            labels={"occupancy_rate": "Taux occupation (%)", "type": "Type"},
        )
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Coordonnées GPS manquantes.")

# ── Centre-ville Citedia ──────────────────────────────────────────────────────
with tab_list:
    cv = df[df["type"] == "Centre-ville"] if "type" in df.columns else df
    if cv.empty:
        st.info("Aucun parking centre-ville.")
    else:
        fig_cv = px.bar(
            cv.sort_values("free_spaces", ascending=True),
            x="free_spaces", y="name", orientation="h",
            color="occupancy_rate", color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            title="Places libres — Parkings centre-ville",
            labels={"free_spaces": "Places libres", "name": "Parking",
                    "occupancy_rate": "Taux occ. (%)"},
            height=420,
            text="free_spaces",
        )
        fig_cv.update_traces(textposition="outside")
        st.plotly_chart(fig_cv, use_container_width=True)

        # Jauge taux d'occupation
        cols_cv = st.columns(min(len(cv), 5))
        for i, (_, row) in enumerate(cv.iterrows()):
            if i >= 5:
                break
            rate = row.get("occupancy_rate", 0) or 0
            color = "#e74c3c" if rate >= 90 else "#f39c12" if rate >= 70 else "#2ecc71"
            cols_cv[i].markdown(
                f"**{row['name']}**  \n"
                f"🟢 {int(row.get('free_spaces',0))} libres / {int(row.get('total_spaces',0))}  \n"
                f"<span style='color:{color}'>**{rate:.0f}%** occupé</span>",
                unsafe_allow_html=True,
            )

# ── Parcs-Relais STAR ─────────────────────────────────────────────────────────
with tab_pr:
    pr = df[df["type"] == "Parc-Relais"] if "type" in df.columns else pd.DataFrame()
    if pr.empty:
        st.info("Aucun parc-relais.")
    else:
        fig_pr = px.bar(
            pr.sort_values("free_spaces", ascending=True),
            x="free_spaces", y="name", orientation="h",
            color="occupancy_rate", color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            title="Places libres — Parcs-Relais P+R",
            labels={"free_spaces": "Places libres", "name": "Parc-Relais"},
            height=380,
            text="free_spaces",
        )
        fig_pr.update_traces(textposition="outside")
        st.plotly_chart(fig_pr, use_container_width=True)

        # Spécificités P+R : EV, covoiturage, PMR
        if "free_ev" in pr.columns:
            st.subheader("Disponibilité spécifique P+R")
            special_cols = st.columns(3)
            with special_cols[0]:
                ev_df = pr[pr["total_ev"].fillna(0) > 0][["name", "free_ev", "total_ev"]]
                if not ev_df.empty:
                    st.markdown("**⚡ Bornes électriques**")
                    st.dataframe(ev_df.rename(columns={"name": "Parc", "free_ev": "Libres", "total_ev": "Total"}), hide_index=True)
            with special_cols[1]:
                cp_df = pr[pr["total_carpool"].fillna(0) > 0][["name", "free_carpool", "total_carpool"]]
                if not cp_df.empty:
                    st.markdown("**🚗 Covoiturage**")
                    st.dataframe(cp_df.rename(columns={"name": "Parc", "free_carpool": "Libres", "total_carpool": "Total"}), hide_index=True)
            with special_cols[2]:
                pmr_df = pr[pr["total_pmr"].fillna(0) > 0][["name", "free_pmr", "total_pmr"]]
                if not pmr_df.empty:
                    st.markdown("**♿ PMR**")
                    st.dataframe(pmr_df.rename(columns={"name": "Parc", "free_pmr": "Libres", "total_pmr": "Total"}), hide_index=True)

# ── Tendances 24h ─────────────────────────────────────────────────────────────
with tab_trends:
    hist = load_history()
    if hist.empty or "snapshot_time" not in hist.columns:
        st.info("Pas encore d'historique. Relancez le pipeline plusieurs fois.")
    else:
        hist = hist.copy()
        hist["snapshot_time"] = to_paris(hist["snapshot_time"])
        avg = hist.groupby("snapshot_time", as_index=False)["free_spaces"].mean()

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=avg["snapshot_time"], y=avg["free_spaces"].round(0),
            name="Places libres (moy.)", mode="lines+markers",
            line={"color": "#2ecc71", "width": 2},
        ))
        if "temperature_c" in hist.columns and hist["temperature_c"].notna().any():
            avg_t = hist.dropna(subset=["temperature_c"]).groupby("snapshot_time", as_index=False)["temperature_c"].mean()
            fig_trend.add_trace(go.Scatter(
                x=avg_t["snapshot_time"], y=avg_t["temperature_c"].round(1),
                name="Température (°C)", mode="lines",
                yaxis="y2", line={"color": "#e67e22", "dash": "dot"},
            ))
            fig_trend.update_layout(yaxis2={"title": "Température (°C)", "overlaying": "y", "side": "right"})

        fig_trend.update_layout(
            title="Évolution des places libres sur 24h (heure de Paris)",
            xaxis_title="Heure", yaxis_title="Places libres (moyenne)",
            height=420,
        )
        st.plotly_chart(fig_trend, use_container_width=True)

        # Par parking
        if "parking_id" in hist.columns and "name" in df.columns:
            hist_named = hist.merge(df[["parking_id", "name"]].drop_duplicates(), on="parking_id", how="left")
            parking_choice = st.selectbox("Détail par parking", hist_named["name"].dropna().unique())
            sub = hist_named[hist_named["name"] == parking_choice]
            fig_sub = px.line(sub, x="snapshot_time", y="free_spaces",
                              markers=True, title=f"Places libres — {parking_choice}",
                              labels={"snapshot_time": "Heure", "free_spaces": "Places libres"})
            st.plotly_chart(fig_sub, use_container_width=True)

# ── Météo ─────────────────────────────────────────────────────────────────────
with tab_weather:
    wh = load_weather_hist()
    last_w = wh.iloc[-1] if not wh.empty else None

    if last_w is not None:
        wc1, wc2, wc3, wc4 = st.columns(4)
        wc1.metric("Température",  f"{last_w.get('temperature_c','—')} °C")
        wc2.metric("Humidité",     f"{last_w.get('humidity_pct','—')} %")
        wc3.metric("Vent",         f"{last_w.get('wind_speed_kmh','—')} km/h")
        wc4.metric("Ciel",         str(last_w.get("weather_description", "—")))

    if not wh.empty and "scraped_at" in wh.columns:
        wh = wh.copy()
        wh["scraped_at"] = to_paris(wh["scraped_at"])
        if wh["temperature_c"].notna().any():
            st.plotly_chart(px.line(wh, x="scraped_at", y="temperature_c", markers=True,
                title="Température (Rennes) — 24h", color_discrete_sequence=["#e74c3c"],
                labels={"scraped_at": "Heure", "temperature_c": "°C"}), use_container_width=True)
        if wh["humidity_pct"].notna().any():
            st.plotly_chart(px.line(wh, x="scraped_at", y="humidity_pct", markers=True,
                title="Humidité — 24h", color_discrete_sequence=["#3498db"],
                labels={"scraped_at": "Heure", "humidity_pct": "%"}), use_container_width=True)
    else:
        st.info("Pas encore d'historique météo.")

# ── Données brutes ────────────────────────────────────────────────────────────
with tab_data:
    cols = [c for c in ["parking_id", "name", "type", "source", "free_spaces",
                         "occupied_spaces", "total_spaces", "occupancy_rate",
                         "is_open", "is_critical", "is_full", "status", "address"] if c in df.columns]
    st.dataframe(df[cols].sort_values("occupancy_rate", ascending=False),
                 use_container_width=True, height=500)
    st.download_button("Télécharger CSV",
                       df[cols].to_csv(index=False).encode("utf-8"),
                       "parkings_rennes.csv", "text/csv")
