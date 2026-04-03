"""
View — Dashboard Streamlit Parkings Rennes
Visualisation temps réel : 10 parkings centre-ville (Citedia) + 8 parcs-relais (STAR P+R)
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

# Auto-refresh toutes les 60 secondes
st_autorefresh(interval=60_000, key="autorefresh")


# ── Chargement des données ────────────────────────────────────────────────────

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


@st.cache_data(ttl=60)
def load_history() -> pd.DataFrame:
    try:
        from src.models.warehouse import load_availability_history
        df = load_availability_history(24)
        if not df.empty:
            return df
    except Exception:
        pass
    from src.models.data_lake import load_parking_history
    return load_parking_history(24)


@st.cache_data(ttl=60)
def load_weather_hist() -> pd.DataFrame:
    try:
        from src.models.warehouse import load_weather_history
        df = load_weather_history(24)
        if not df.empty:
            return df
    except Exception:
        pass
    from src.models.data_lake import load_weather_history
    return load_weather_history(24)


def to_paris(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(PARIS_TZ)


def color_status(rate):
    if rate >= 90:
        return "🔴"
    elif rate >= 70:
        return "🟡"
    else:
        return "🟢"


# ── Données ───────────────────────────────────────────────────────────────────

df = load_parkings()

if df.empty:
    st.error("Aucune donnée. Lancez `python -m src.controllers.pipeline`.")
    st.stop()

df_cv = df[df["type"] == "Centre-ville"].copy()
df_pr = df[df["type"] == "Parc-Relais"].copy()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🅿️ Parkings Rennes")
    st.markdown("---")

    type_filter = st.multiselect(
        "Type de parking",
        ["Centre-ville", "Parc-Relais"],
        default=["Centre-ville", "Parc-Relais"],
    )
    show_only_open   = st.checkbox("Ouverts uniquement", True)
    show_only_avail  = st.checkbox("Masquer les complets", False)
    search = st.text_input("🔍 Rechercher", "")

    st.markdown("---")
    if st.button("🔄 Rafraîchir"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Auto-refresh toutes les 60 s.")

    # Météo dans la sidebar
    st.markdown("---")
    st.markdown("### 🌤 Météo Rennes")
    temp  = df["temperature_c"].iloc[0]       if "temperature_c"       in df.columns else None
    hum   = df["humidity_pct"].iloc[0]        if "humidity_pct"         in df.columns else None
    wind  = df["wind_speed_kmh"].iloc[0]      if "wind_speed_kmh"       in df.columns else None
    desc  = df["weather_description"].iloc[0] if "weather_description"  in df.columns else None
    if temp is not None:
        st.metric("Température", f"{temp} °C")
        st.metric("Humidité",    f"{hum} %")
        st.metric("Vent",        f"{wind} km/h")
        st.caption(str(desc) if desc else "")

# Filtres
df_filtered = df.copy()
if type_filter:
    df_filtered = df_filtered[df_filtered["type"].isin(type_filter)]
if show_only_open and "is_open" in df_filtered.columns:
    df_filtered = df_filtered[df_filtered["is_open"]]
if show_only_avail and "is_full" in df_filtered.columns:
    df_filtered = df_filtered[~df_filtered["is_full"]]
if search:
    df_filtered = df_filtered[df_filtered["name"].str.contains(search, case=False, na=False)]


# ── Header ────────────────────────────────────────────────────────────────────

st.title("🅿️ Parkings Rennes — Disponibilité en temps réel")

if "snapshot_time" in df.columns:
    last_ts = pd.to_datetime(df["snapshot_time"], utc=True).max()
    if pd.notna(last_ts):
        st.caption(f"Dernière mise à jour : **{last_ts.astimezone(PARIS_TZ).strftime('%d/%m/%Y à %H:%M')}**")

# ── KPIs globaux ─────────────────────────────────────────────────────────────

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("🅿️ Parkings ouverts",  int(df["is_open"].sum()))
k2.metric("✅ Places libres",      int(df["free_spaces"].sum()))
k3.metric("🚗 Places occupées",    int(df["occupied_spaces"].sum()))
k4.metric("⚠️ Parkings critiques", int(df["is_critical"].sum()),
          delta=f"{int(df['is_critical'].sum())} presque pleins", delta_color="inverse")
k5.metric("🔴 Parkings complets",  int(df["is_full"].sum()),
          delta_color="inverse")

st.divider()

# ── Onglets ───────────────────────────────────────────────────────────────────

tab_carte, tab_cv, tab_pr, tab_trends, tab_meteo, tab_lake = st.tabs([
    "🗺️ Carte", "🏙️ Centre-ville", "🚌 Parcs-Relais", "📈 Tendances 24h", "🌤️ Météo", "🗄️ Data Lake"
])


# ── CARTE ─────────────────────────────────────────────────────────────────────
with tab_carte:
    map_df = df_filtered.dropna(subset=["lat", "lon"]).copy()

    if map_df.empty:
        st.info("Pas de coordonnées GPS disponibles.")
    else:
        map_df["taille"] = map_df["free_spaces"].clip(lower=10)
        map_df["label_statut"] = map_df.apply(
            lambda r: "🔴 Complet" if r.get("is_full")
            else ("⚠️ Critique" if r.get("is_critical") else "🟢 Disponible"), axis=1
        )

        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat", lon="lon",
            size="taille",
            color="occupancy_rate",
            color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            hover_name="name",
            hover_data={
                "free_spaces":    ":.0f",
                "total_spaces":   ":.0f",
                "occupancy_rate": ":.1f",
                "type":           True,
                "label_statut":   True,
                "taille":         False,
                "lat":            False,
                "lon":            False,
            },
            mapbox_style="open-street-map",
            zoom=12,
            center={"lat": 48.109, "lon": -1.677},
            height=580,
            labels={
                "occupancy_rate": "Taux occupation (%)",
                "free_spaces":    "Places libres",
                "total_spaces":   "Capacité totale",
                "type":           "Type",
                "label_statut":   "Statut",
            },
        )
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},
                              coloraxis_colorbar_title="Occupation %")
        st.plotly_chart(fig_map, use_container_width=True)

        lc1, lc2, lc3 = st.columns(3)
        lc1.success("🟢 Disponible  (< 70%)")
        lc2.warning("🟡 Quasi-plein (70-90%)")
        lc3.error("🔴 Critique / Complet (> 90%)")


# ── CENTRE-VILLE ──────────────────────────────────────────────────────────────
with tab_cv:
    st.subheader("🏙️ Parkings centre-ville (Citedia)")

    if df_cv.empty:
        st.info("Aucune donnée centre-ville.")
    else:
        cols = st.columns(5)
        for i, (_, row) in enumerate(df_cv.iterrows()):
            rate  = float(row.get("occupancy_rate", 0) or 0)
            free  = int(row.get("free_spaces", 0) or 0)
            total = int(row.get("total_spaces", 0) or 0)
            icon  = color_status(rate)
            with cols[i % 5]:
                st.markdown(f"**{row['name']}**")
                st.progress(min(rate / 100, 1.0))
                st.caption(f"{icon} {free} / {total} places libres — **{rate:.0f}%** occupé")

        st.divider()

        cv_sorted = df_cv.sort_values("free_spaces")
        fig_cv = px.bar(
            cv_sorted,
            x="free_spaces", y="name",
            orientation="h",
            color="occupancy_rate",
            color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            text="free_spaces",
            title="Places libres par parking — Centre-ville",
            labels={"free_spaces": "Places libres", "name": "Parking",
                    "occupancy_rate": "Taux occ. (%)"},
            height=420,
        )
        fig_cv.update_traces(texttemplate="%{text} places", textposition="outside")
        fig_cv.update_layout(xaxis_range=[0, df_cv["total_spaces"].max() * 1.15])
        st.plotly_chart(fig_cv, use_container_width=True)

        st.dataframe(
            df_cv[["name", "free_spaces", "occupied_spaces", "total_spaces", "occupancy_rate", "status"]]
            .rename(columns={
                "name": "Parking", "free_spaces": "Libres",
                "occupied_spaces": "Occupées", "total_spaces": "Total",
                "occupancy_rate": "Taux occ. (%)", "status": "Statut"
            }).sort_values("Taux occ. (%)", ascending=False).reset_index(drop=True),
            use_container_width=True, hide_index=True,
        )


# ── PARCS-RELAIS ──────────────────────────────────────────────────────────────
with tab_pr:
    st.subheader("🚌 Parcs-Relais P+R (STAR)")

    if df_pr.empty:
        st.info("Aucune donnée parc-relais.")
    else:
        cols_pr = st.columns(4)
        for i, (_, row) in enumerate(df_pr.iterrows()):
            rate  = float(row.get("occupancy_rate", 0) or 0)
            free  = int(row.get("free_spaces", 0) or 0)
            total = int(row.get("total_spaces", 0) or 0)
            icon  = color_status(rate)
            with cols_pr[i % 4]:
                st.markdown(f"**{row['name']}**")
                st.progress(min(rate / 100, 1.0))
                st.caption(f"{icon} {free} / {total} — **{rate:.0f}%**")

        st.divider()

        fig_pr = px.bar(
            df_pr.sort_values("free_spaces"),
            x="free_spaces", y="name",
            orientation="h",
            color="occupancy_rate",
            color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            text="free_spaces",
            title="Places libres par parc-relais",
            labels={"free_spaces": "Places libres", "name": "Parc-Relais",
                    "occupancy_rate": "Taux occ. (%)"},
            height=380,
        )
        fig_pr.update_traces(texttemplate="%{text} places", textposition="outside")
        st.plotly_chart(fig_pr, use_container_width=True)

        st.subheader("Disponibilités spécifiques")
        sp1, sp2, sp3 = st.columns(3)

        with sp1:
            st.markdown("**⚡ Véhicules électriques**")
            if "total_ev" in df_pr.columns and "free_ev" in df_pr.columns:
                ev = df_pr[df_pr["total_ev"].fillna(0) > 0][["name", "free_ev", "total_ev"]]
            else:
                ev = pd.DataFrame()
            if not ev.empty:
                fig_ev = px.bar(ev, x="name", y=["free_ev", "total_ev"],
                                barmode="overlay", color_discrete_sequence=["#2ecc71", "#bdc3c7"],
                                labels={"value": "Places", "name": "Parc"}, height=280)
                fig_ev.update_layout(showlegend=False, margin={"t": 10})
                st.plotly_chart(fig_ev, use_container_width=True)
            else:
                st.info("Aucune borne VE.")

        with sp2:
            st.markdown("**🚗 Covoiturage**")
            if "total_carpool" in df_pr.columns and "free_carpool" in df_pr.columns:
                cp = df_pr[df_pr["total_carpool"].fillna(0) > 0][["name", "free_carpool", "total_carpool"]]
            else:
                cp = pd.DataFrame()
            if not cp.empty:
                fig_cp = px.bar(cp, x="name", y=["free_carpool", "total_carpool"],
                                barmode="overlay", color_discrete_sequence=["#3498db", "#bdc3c7"],
                                labels={"value": "Places", "name": "Parc"}, height=280)
                fig_cp.update_layout(showlegend=False, margin={"t": 10})
                st.plotly_chart(fig_cp, use_container_width=True)
            else:
                st.info("Aucune place covoiturage.")

        with sp3:
            st.markdown("**♿ PMR**")
            if "total_pmr" in df_pr.columns and "free_pmr" in df_pr.columns:
                pmr = df_pr[df_pr["total_pmr"].fillna(0) > 0][["name", "free_pmr", "total_pmr"]]
            else:
                pmr = pd.DataFrame()
            if not pmr.empty:
                fig_pmr = px.bar(pmr, x="name", y=["free_pmr", "total_pmr"],
                                 barmode="overlay", color_discrete_sequence=["#9b59b6", "#bdc3c7"],
                                 labels={"value": "Places", "name": "Parc"}, height=280)
                fig_pmr.update_layout(showlegend=False, margin={"t": 10})
                st.plotly_chart(fig_pmr, use_container_width=True)
            else:
                st.info("Aucune place PMR.")


# ── TENDANCES 24h ─────────────────────────────────────────────────────────────
with tab_trends:
    hist = load_history()

    if hist.empty or "snapshot_time" not in hist.columns:
        st.info("Pas encore d'historique. Relancez le pipeline plusieurs fois (toutes les 15 min avec Airflow).")
    else:
        hist = hist.copy()
        hist["snapshot_time"] = to_paris(hist["snapshot_time"])

        avg = hist.groupby("snapshot_time", as_index=False)["free_spaces"].mean()
        avg["free_spaces"] = avg["free_spaces"].round(0)

        fig_global = go.Figure()
        fig_global.add_trace(go.Scatter(
            x=avg["snapshot_time"], y=avg["free_spaces"],
            name="Places libres (moy. tous parkings)",
            mode="lines+markers",
            line={"color": "#2ecc71", "width": 2},
            fill="tozeroy", fillcolor="rgba(46,204,113,0.1)",
        ))

        if "temperature_c" in hist.columns and hist["temperature_c"].notna().any():
            avg_t = hist.dropna(subset=["temperature_c"]).groupby(
                "snapshot_time", as_index=False)["temperature_c"].mean()
            fig_global.add_trace(go.Scatter(
                x=avg_t["snapshot_time"], y=avg_t["temperature_c"].round(1),
                name="Température (°C)", mode="lines",
                yaxis="y2", line={"color": "#e67e22", "dash": "dot", "width": 2},
            ))
            fig_global.update_layout(
                yaxis2={"title": "Température (°C)", "overlaying": "y", "side": "right"})

        fig_global.update_layout(
            title="Évolution des places libres (tous parkings) — 24h",
            xaxis_title="Heure (Paris)", yaxis_title="Places libres (moyenne)",
            height=400, legend={"x": 0.01, "y": 0.99},
        )
        st.plotly_chart(fig_global, use_container_width=True)

        st.subheader("Détail par parking")
        if "parking_id" in hist.columns and "name" in df.columns:
            names = df[["parking_id", "name"]].drop_duplicates()
            hist_n = hist.merge(names, on="parking_id", how="left")
            choice = st.selectbox("Choisir un parking", sorted(hist_n["name"].dropna().unique()))
            sub = hist_n[hist_n["name"] == choice].sort_values("snapshot_time")
            fig_sub = px.area(
                sub, x="snapshot_time", y="free_spaces",
                title=f"Places libres — {choice}",
                labels={"snapshot_time": "Heure", "free_spaces": "Places libres"},
                color_discrete_sequence=["#3498db"],
                height=320,
            )
            st.plotly_chart(fig_sub, use_container_width=True)

        if "occupancy_rate" in hist.columns and "parking_id" in hist.columns:
            st.subheader("Heatmap — taux d'occupation par parking")
            pivot = hist.merge(df[["parking_id", "name"]].drop_duplicates(), on="parking_id", how="left")
            pivot["heure"] = pivot["snapshot_time"].dt.strftime("%H:%M")
            heat = pivot.pivot_table(index="name", columns="heure",
                                     values="occupancy_rate", aggfunc="mean").round(1)
            fig_heat = px.imshow(
                heat, color_continuous_scale="RdYlGn_r",
                range_color=[0, 100], aspect="auto",
                title="Taux d'occupation moyen (%) par heure",
                labels={"color": "Occ. %"},
                height=420,
            )
            st.plotly_chart(fig_heat, use_container_width=True)


# ── MÉTÉO ─────────────────────────────────────────────────────────────────────
with tab_meteo:
    wh = load_weather_hist()

    if not wh.empty and "scraped_at" in wh.columns:
        wh = wh.copy()
        wh["scraped_at"] = to_paris(wh["scraped_at"])
        last_w = wh.iloc[-1]

        wm1, wm2, wm3, wm4 = st.columns(4)
        wm1.metric("🌡️ Température", f"{last_w.get('temperature_c', '—')} °C")
        wm2.metric("💧 Humidité",    f"{last_w.get('humidity_pct', '—')} %")
        wm3.metric("💨 Vent",        f"{last_w.get('wind_speed_kmh', '—')} km/h")
        wm4.metric("🌤️ Ciel",        str(last_w.get("weather_description", "—")))

        st.divider()

        col_t, col_h = st.columns(2)
        with col_t:
            if wh["temperature_c"].notna().any():
                fig_t = px.line(wh, x="scraped_at", y="temperature_c",
                                markers=True, title="Température sur 24h",
                                labels={"scraped_at": "Heure", "temperature_c": "°C"},
                                color_discrete_sequence=["#e74c3c"], height=320)
                fig_t.update_layout(margin={"t": 40})
                st.plotly_chart(fig_t, use_container_width=True)

        with col_h:
            if wh["humidity_pct"].notna().any():
                fig_h = px.line(wh, x="scraped_at", y="humidity_pct",
                                markers=True, title="Humidité sur 24h",
                                labels={"scraped_at": "Heure", "humidity_pct": "%"},
                                color_discrete_sequence=["#3498db"], height=320)
                fig_h.update_layout(margin={"t": 40})
                st.plotly_chart(fig_h, use_container_width=True)

        if wh["wind_speed_kmh"].notna().any():
            fig_w = px.bar(wh, x="scraped_at", y="wind_speed_kmh",
                           title="Vitesse du vent sur 24h",
                           labels={"scraped_at": "Heure", "wind_speed_kmh": "km/h"},
                           color_discrete_sequence=["#95a5a6"], height=280)
            st.plotly_chart(fig_w, use_container_width=True)
    else:
        st.info("Pas encore d'historique météo. Relancez le pipeline.")


# ── DATA LAKE ─────────────────────────────────────────────────────────────────
with tab_lake:
    st.subheader("🗄️ Data Lake — MinIO")
    st.caption("Buckets S3-compatibles : snapshots bruts (JSON) et données transformées (CSV/Parquet).")

    try:
        from src.models.data_lake import list_objects, load_raw_preview, BUCKET_RAW, BUCKET_PROCESSED

        col_raw, col_proc = st.columns(2)

        with col_raw:
            st.markdown(f"**Bucket `{BUCKET_RAW}`**")
            try:
                objects_raw = list_objects(BUCKET_RAW)
            except Exception:
                objects_raw = []

            if objects_raw:
                df_raw_objects = pd.DataFrame(objects_raw)
                st.dataframe(df_raw_objects, use_container_width=True, hide_index=True)

                selected = st.selectbox(
                    "Prévisualiser un fichier (5 premières entrées)",
                    options=[o["key"] for o in objects_raw],
                    key="lake_raw_select",
                )
                if selected:
                    try:
                        preview = load_raw_preview(selected)
                        st.json(preview[:5] if isinstance(preview, list) else preview)
                    except Exception as exc:
                        st.error(f"Impossible de charger le fichier : {exc}")
            else:
                st.info("Aucun fichier dans ce bucket.")

        with col_proc:
            st.markdown(f"**Bucket `{BUCKET_PROCESSED}`**")
            try:
                objects_proc = list_objects(BUCKET_PROCESSED)
            except Exception:
                objects_proc = []

            if objects_proc:
                st.dataframe(pd.DataFrame(objects_proc), use_container_width=True, hide_index=True)
            else:
                st.info("Aucun fichier traité.")

        st.markdown("---")
        st.caption(
            "Console MinIO → [http://localhost:9001](http://localhost:9001) "
            "(minioadmin / minioadmin)"
        )

    except Exception as e:
        st.warning(
            f"MinIO non disponible en mode local. "
            f"Lancez `docker compose up minio -d` pour accéder au Data Lake.\n\n"
            f"Détail : `{e}`"
        )
