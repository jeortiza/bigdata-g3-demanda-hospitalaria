# ============================================================
#  DASHBOARD EJECUTIVO — DEMANDA HOSPITALARIA ESSALUD
#  Grupo 3 · Big Data DD283 · Junior Ortiz
#  Ejecutar con:  streamlit run src/dashboard.py
# ============================================================

from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(
    page_title="Demanda Hospitalaria EsSalud — Grupo 3",
    page_icon="🏥",
    layout="wide",
)

# ------------------------------------------------------------
#  Carga de datos
# ------------------------------------------------------------
@st.cache_data
def localizar_gold():
    aqui = Path(__file__).resolve().parent
    raiz = next((p for p in [aqui, *aqui.parents]
                 if (p / "data" / "gold").is_dir()), None)
    return (raiz / "data" / "gold") if raiz else None

RUTA_GOLD = localizar_gold()

@st.cache_data
def cargar(nombre):
    return pd.read_parquet(RUTA_GOLD / nombre)

# ------------------------------------------------------------
#  Barra lateral — navegacion
# ------------------------------------------------------------
with st.sidebar:
    st.title("🏥 EsSalud")
    st.caption("Predicción de Demanda Hospitalaria")
    st.markdown("**Grupo 3** · Big Data DD283")
    st.divider()

    vista = st.radio(
        "Vista",
        ["📊 Resumen — KPIs",
         "👔 Director de Hospital",
         "🗺️ Gerente de Red",
         "🦟 Epidemiólogo"],
        label_visibility="collapsed",
    )
    st.divider()
    st.caption("Lima Metropolitana · 2022–2024  \n"
               "Horizonte de predicción: 4 semanas")

# ------------------------------------------------------------
#  Verificacion de datos
# ------------------------------------------------------------
if RUTA_GOLD is None or not RUTA_GOLD.exists():
    st.error("No se encontró data/gold/. Ejecuta primero el notebook 07.")
    st.stop()

# ============================================================
#  VISTA: RESUMEN — KPIs
# ============================================================
if vista.startswith("📊"):
    st.title("Sistema de Predicción de Demanda Hospitalaria")
    st.caption("EsSalud — Lima Metropolitana · Predicción a 4 semanas, "
               "KPIs de gestión y vigilancia epidemiológica.")
    st.divider()

    st.header("KPIs de Gestión Hospitalaria")
    st.caption("Definiciones y metas según README §7.4. Miden un hospital "
               "sintético: los valores fuera de meta reflejan el dataset.")

    kpis = cargar("kpis_dashboard.parquet")
    for grupo in [kpis.iloc[i:i+4] for i in range(0, len(kpis), 4)]:
        cols = st.columns(len(grupo))
        for col, (_, kpi) in zip(cols, grupo.iterrows()):
            en_meta = kpi["estado"] == "EN META"
            col.metric(kpi["KPI"], kpi["valor"],
                       delta=f"Meta: {kpi['meta_essalud']}",
                       delta_color="normal" if en_meta else "inverse")
            col.caption(f"{'✅' if en_meta else '⚠️'} {kpi['estado']} · "
                        f"{kpi['fuente']}")

# ============================================================
#  VISTA: DIRECTOR
# ============================================================
elif vista.startswith("👔"):
    st.title("👔 Director de Hospital")
    st.caption("Demanda actual vs predicción de las próximas 4 semanas. "
               "Alertas ante desviaciones ≥15% frente a la media histórica.")

    v1 = cargar("vista1_director.parquet")
    esp = st.selectbox("Especialidad", sorted(v1["especialidad"].unique()))
    d = v1[v1["especialidad"] == esp].sort_values("horizonte")

    c1, c2, c3 = st.columns(3)
    c1.metric("Demanda actual", f"{d['demanda_actual'].iloc[0]:,.0f}")
    c2.metric("Predicción +1 semana", f"{d['prediccion'].iloc[0]:,.0f}",
              delta=f"{d['variacion_pct'].iloc[0]:+.1f}% vs media")
    alerta = d["alerta"].iloc[0]
    c3.metric("Estado", "🟢 Normal" if alerta == "normal"
              else ("🔴 Alza" if "ALZA" in alerta else "🟡 Baja"))

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["semana"], y=d["pred_max_80"], mode="lines",
                             line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=d["semana"], y=d["pred_min_80"], mode="lines",
                             fill="tonexty", fillcolor="rgba(70,130,180,0.2)",
                             line=dict(width=0), name="Intervalo 80%"))
    fig.add_trace(go.Scatter(x=d["semana"], y=d["prediccion"],
                             mode="lines+markers",
                             line=dict(color="steelblue", width=3),
                             name="Predicción"))
    fig.add_hline(y=d["media_historica"].iloc[0], line_dash="dash",
                  line_color="gray", annotation_text="Media histórica")
    fig.update_layout(height=420, xaxis_title="Semana",
                      yaxis_title="Atenciones", margin=dict(t=20, b=20),
                      hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("##### Alertas activas en la red")
    alertas = v1[v1["alerta"] != "normal"][
        ["especialidad", "semana", "prediccion", "variacion_pct", "alerta"]]
    if len(alertas):
        st.dataframe(alertas.reset_index(drop=True), use_container_width=True)
    else:
        st.success("Sin alertas: demanda proyectada en rango normal.")

# ============================================================
#  VISTA: GERENTE DE RED — MAPA DE LIMA
# ============================================================
elif vista.startswith("🗺️"):
    st.title("🗺️ Gerente de Red Asistencial")
    st.caption("Demanda proyectada por establecimiento y redistribución "
               "recomendada de recursos. Coordenadas reales de los "
               "hospitales de EsSalud en Lima.")

    import folium
    from streamlit_folium import st_folium

    v2 = cargar("vista2_mapa.parquet")

    # --- Metricas resumen de la red ---
    cols = st.columns(3)
    for col, (_, est) in zip(cols, v2.iterrows()):
        color = ("🔴" if est["nivel_presion"] == "ALTA"
                 else "🟡" if est["nivel_presion"] == "MEDIA" else "🟢")
        col.metric(
            f"{color} {est['establecimiento'].replace('Hospital ', '')}",
            f"{est['demanda_predicha']:,.0f} atenc.",
            delta=f"brecha {est['brecha']:+,.0f}",
            delta_color="inverse")
        col.caption(f"{est['medicos_activos']} médicos · "
                    f"{est['camas_totales']} camas")

    st.divider()
    c_map, c_info = st.columns([2, 1])

    # --- Mapa Folium ---
    with c_map:
        centro = [float(v2["lat"].mean()), float(v2["lon"].mean())]
        m = folium.Map(location=centro, zoom_start=12, tiles="CartoDB positron")

        col_nivel = {"ALTA": "red", "MEDIA": "orange", "HOLGADA": "green"}
        max_dem = float(v2["demanda_predicha"].max())
        for _, est in v2.iterrows():
            radio = 8 + (float(est["demanda_predicha"]) / max_dem) * 22
            texto = (f"{est['establecimiento']} | "
                     f"Demanda: {est['demanda_predicha']:,.0f} | "
                     f"Brecha: {est['brecha']:+,.0f} | "
                     f"Presion: {est['nivel_presion']}")
            folium.CircleMarker(
                location=[float(est["lat"]), float(est["lon"])],
                radius=radio,
                color=col_nivel.get(est["nivel_presion"], "gray"),
                fill=True,
                fill_color=col_nivel.get(est["nivel_presion"], "gray"),
                fill_opacity=0.6,
                tooltip=texto,
            ).add_to(m)

        st_folium(m, width=700, height=450, returned_objects=[])

    # --- Panel de redistribucion ---
    with c_info:
        st.markdown("##### Redistribución recomendada")
        for _, est in v2.iterrows():
            icono = ("🔴" if est["nivel_presion"] == "ALTA"
                     else "🟡" if est["nivel_presion"] == "MEDIA" else "🟢")
            st.markdown(f"**{icono} {est['establecimiento'].replace('Hospital ','')}**")
            st.caption(f"{est['recomendacion']}")
        st.divider()
        st.caption("El tamaño del círculo representa la demanda proyectada; "
                   "el color, el nivel de presión sobre la oferta.")

    st.markdown("##### Detalle por establecimiento")
    st.dataframe(
        v2[["establecimiento", "distrito", "demanda_predicha", "oferta_semanal",
            "brecha", "nivel_presion", "recomendacion"]],
        use_container_width=True, hide_index=True)

# ============================================================
#  VISTA: EPIDEMIOLOGO
# ============================================================
elif vista.startswith("🦟"):
    st.title("🦟 Vigilancia Epidemiológica")
    st.info("Correlación clima-enfermedad y vigilancia — en construcción.")