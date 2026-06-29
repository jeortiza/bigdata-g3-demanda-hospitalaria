import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# ============================================================
#  CONFIGURACION GENERAL
# ============================================================
random.seed(42)
np.random.seed(42)

FECHA_INICIO = datetime(2022, 1, 1)
FECHA_FIN    = datetime(2024, 12, 31)

DIAGNOSTICOS = {
    "A90": ("Dengue (sin signo de alarma)",        "Enfermedades_infecciosas", "Emergencia"),
    "J11": ("Influenza",                            "Enfermedades_infecciosas", "Medicina Interna"),
    "J06": ("Infeccion aguda vias respiratorias",   "Respiratorias",            "Medicina Interna"),
    "I10": ("Hipertension esencial",                "Cardiovascular",           "Cardiologia"),
    "E11": ("Diabetes mellitus tipo 2",             "Cronicas",                 "Medicina Interna"),
    "S00-S99": ("Traumatismos",                     "Traumatologia",            "Traumatologia"),
}

PESOS_MENSUALES = {
    "A90":     [10,  11,  12,   7,   4,   2,   2,   2,   3,   4,   6,   8],
    "J11":     [ 2,   2,   3,   5,   7,  10,  12,  10,   7,   4,   3,   2],
    "J06":     [ 4,   4,   5,   6,   7,   8,   9,   8,   6,   5,   4,   4],
    "I10":     [ 6,   6,   6,   6,   6,   6,   6,   6,   6,   6,   6,   6],
    "E11":     [ 6,   6,   6,   6,   6,   6,   6,   6,   6,   6,   6,   6],
    "S00-S99": [ 5,   5,   5,   5,   5,   5,   5,   5,   5,   5,   5,   5],
}

CLIMA_LIMA = {
    1:(28.0,1.0), 2:(28.5,0.5), 3:(28.0,0.5),  4:(26.0,0.5),
    5:(23.0,1.0), 6:(20.0,1.5), 7:(19.0,2.0),  8:(19.0,2.0),
    9:(20.0,1.5), 10:(22.0,1.0), 11:(24.0,0.5), 12:(26.0,0.5),
}

ESTABLECIMIENTOS = {
    "HOSP-001": ("Hospital Almenara",   "Lima Metropolitana", 450),
    "HOSP-002": ("Hospital Rebagliati", "Lima Metropolitana", 520),
    "HOSP-003": ("Hospital Sabogal",    "Lima Metropolitana", 380),
}

DISTRITOS_LIMA = ["La Victoria", "San Juan de Lurigancho", "Comas", "Ate",
                  "San Martin de Porres", "Villa El Salvador", "Surco", "Callao"]

TIPOS_ATENCION = ["consulta_externa", "emergencia", "hospitalizacion"]


# ============================================================
#  FUNCIONES AUXILIARES
# ============================================================
def generar_clima(mes):
    temp_base, lluvia_base = CLIMA_LIMA[mes]
    temperatura   = round(temp_base + np.random.normal(0, 1.5), 1)
    precipitacion = round(max(0, lluvia_base + np.random.normal(0, 0.5)), 1)
    humedad       = int(np.clip(np.random.normal(80, 8), 60, 95))
    return temperatura, precipitacion, humedad


def semana_epidemiologica(fecha):
    return fecha.isocalendar()[1]


def grupo_etario(edad):
    if edad < 12:   return "0-11"
    if edad < 18:   return "12-17"
    if edad < 30:   return "18-29"
    if edad < 60:   return "30-59"
    return "60+"


def elegir_diagnostico(mes):
    codigos = list(PESOS_MENSUALES.keys())
    pesos   = [PESOS_MENSUALES[c][mes - 1] for c in codigos]
    return random.choices(codigos, weights=pesos, k=1)[0]


# ============================================================
#  GENERADOR PRINCIPAL: ATENCIONES
# ============================================================
def generar_atenciones(num_registros=500000):
    print(f"Generando {num_registros:,} atenciones...")
    datos = []
    dias_totales = (FECHA_FIN - FECHA_INICIO).days

    for i in range(num_registros):
        fecha = FECHA_INICIO + timedelta(days=random.randint(0, dias_totales))
        mes = fecha.month
        cie10 = elegir_diagnostico(mes)
        nombre_diag, grupo_diag, especialidad = DIAGNOSTICOS[cie10]
        est_id = random.choice(list(ESTABLECIMIENTOS.keys()))
        est_nombre, red, camas_tot = ESTABLECIMIENTOS[est_id]
        temp, lluvia, humedad = generar_clima(mes)
        edad = random.randint(1, 90)
        sexo = random.choice(["M", "F"])

        if cie10 in ("A90", "S00-S99"):
            tipo = random.choices(TIPOS_ATENCION, weights=[2, 6, 2])[0]
        else:
            tipo = random.choices(TIPOS_ATENCION, weights=[6, 2, 2])[0]

        dias_estancia = random.randint(1, 12) if tipo == "hospitalizacion" else 0
        ocupadas = int(camas_tot * random.uniform(0.70, 1.15))
        tasa_ocupacion = round(ocupadas / camas_tot, 2)

        registro = {
            "atencion_id":            f"ATN-{fecha.year}-{str(i).zfill(7)}",
            "fecha_atencion":         fecha.strftime("%Y-%m-%d"),
            "semana_epidemiologica":  semana_epidemiologica(fecha),
            "establecimiento_id":     est_id,
            "establecimiento_nombre": est_nombre,
            "red_asistencial":        red,
            "tipo_atencion":          tipo,
            "especialidad":           especialidad,
            "medico_id":              f"MED-{random.randint(1000, 9999)}",
            "paciente_edad":          edad,
            "paciente_grupo_etario":  grupo_etario(edad),
            "paciente_sexo":          sexo,
            "paciente_distrito":      random.choice(DISTRITOS_LIMA),
            "diagnostico_cie10":      cie10,
            "diagnostico_nombre":     nombre_diag,
            "diagnostico_grupo":      grupo_diag,
            "dias_estancia":          dias_estancia,
            "camas_disponibles_dia":  camas_tot,
            "camas_ocupadas_dia":     ocupadas,
            "tasa_ocupacion":         tasa_ocupacion,
            "resultado_atencion":     random.choices(
                                          ["alta", "fallecido", "transferido", "fuga"],
                                          weights=[90, 3, 5, 2])[0],
            "costo_atencion_soles":   round(random.uniform(80, 1500), 2),
            "lista_espera_dias":      0 if tipo == "emergencia" else random.randint(0, 45),
            "temperatura_lima_max":   temp,
            "precipitacion_mm":       lluvia,
            "indice_humedad":         humedad,
            "casos_dengue_semana":    0,
            "alerta_epidemiologica":  False,
        }
        datos.append(registro)

        if (i + 1) % 100000 == 0:
            print(f"   ... {i + 1:,} registros generados")

    return pd.DataFrame(datos)


def generar_clima_semanal():
    print("Generando archivo de clima semanal...")
    filas = []
    fecha = FECHA_INICIO
    while fecha <= FECHA_FIN:
        mes = fecha.month
        temp, lluvia, humedad = generar_clima(mes)
        filas.append({
            "semana":              f"{fecha.year}-W{semana_epidemiologica(fecha):02d}",
            "fecha":               fecha.strftime("%Y-%m-%d"),
            "temperatura_max":     temp,
            "temperatura_min":     round(temp - random.uniform(6, 9), 1),
            "precipitacion_mm":    lluvia,
            "indice_humedad":      humedad,
        })
        fecha += timedelta(days=7)
    return pd.DataFrame(filas)


def generar_epidemiologia_semanal(df_clima):
    print("Generando archivo de epidemiologia semanal...")
    filas = []
    casos_por_semana = {}
    for _, row in df_clima.iterrows():
        temp = row["temperatura_max"]
        base_dengue = max(0, (temp - 20) * 35)
        casos_dengue = int(max(0, base_dengue + np.random.normal(0, 25)))
        base_influenza = max(0, (24 - temp) * 30)
        casos_influenza = int(max(0, base_influenza + np.random.normal(0, 20)))
        alerta = casos_dengue > 250
        filas.append({
            "semana":             row["semana"],
            "fecha":              row["fecha"],
            "casos_dengue":       casos_dengue,
            "casos_influenza":    casos_influenza,
            "alerta_dengue":      alerta,
        })
        casos_por_semana[row["semana"]] = (casos_dengue, alerta)
    return pd.DataFrame(filas), casos_por_semana


def generar_oferta():
    print("Generando archivo de oferta hospitalaria...")
    filas = []
    fecha = FECHA_INICIO
    while fecha <= FECHA_FIN:
        semana = f"{fecha.year}-W{semana_epidemiologica(fecha):02d}"
        for est_id, (nombre, red, camas_tot) in ESTABLECIMIENTOS.items():
            programadas = random.randint(1500, 2200)
            ejecutadas  = int(programadas * random.uniform(0.85, 1.25))
            filas.append({
                "semana":                semana,
                "establecimiento_id":    est_id,
                "camas_totales":         camas_tot,
                "camas_disponibles":     int(camas_tot * random.uniform(0.30, 0.45)),
                "medicos_activos":       random.randint(30, 60),
                "enfermeras_activas":    random.randint(90, 150),
                "consultas_programadas": programadas,
                "consultas_ejecutadas":  ejecutadas,
                "productividad_medica":  round(ejecutadas / random.randint(35, 50), 1),
            })
        fecha += timedelta(days=7)
    return pd.DataFrame(filas)


def main(num=500000):
    os.makedirs("data/raw", exist_ok=True)
    df_atenciones = generar_atenciones(num)
    df_clima = generar_clima_semanal()
    df_epidemio, casos_por_semana = generar_epidemiologia_semanal(df_clima)

    print("Conectando casos de dengue con las atenciones...")
    df_atenciones["_semana"] = (
        df_atenciones["fecha_atencion"].str[:4] + "-W" +
        df_atenciones["semana_epidemiologica"].astype(str).str.zfill(2)
    )
    df_atenciones["casos_dengue_semana"] = df_atenciones["_semana"].map(
        lambda s: casos_por_semana.get(s, (0, False))[0]
    )
    df_atenciones["alerta_epidemiologica"] = df_atenciones["_semana"].map(
        lambda s: casos_por_semana.get(s, (0, False))[1]
    )
    df_atenciones = df_atenciones.drop(columns=["_semana"])

    df_oferta = generar_oferta()

    df_atenciones.to_csv("data/raw/atenciones_essalud.csv", index=False, encoding="utf-8-sig")
    df_clima.to_csv("data/raw/clima_lima_2022_2024.csv", index=False, encoding="utf-8-sig")
    df_epidemio.to_csv("data/raw/epidemio_minsa_2022_2024.csv", index=False, encoding="utf-8-sig")
    df_oferta.to_csv("data/raw/oferta_hospitalaria.csv", index=False, encoding="utf-8-sig")

    print(f"\nEXITO. Atenciones={len(df_atenciones):,} Clima={len(df_clima)} Epidemio={len(df_epidemio)} Oferta={len(df_oferta)}")
    return df_atenciones, df_clima, df_epidemio, df_oferta


if __name__ == "__main__":
    main()