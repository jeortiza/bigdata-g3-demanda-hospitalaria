import os

# --- Configuracion Hadoop/winutils para Windows ---
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\hadoop\bin"
# ---------------------------------------------------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (col, count, trim, concat, lit, lpad, avg,
                                   month, year, when, to_date, weekofyear,
                                   upper, substring, lag, row_number,
                                   monotonically_increasing_id, countDistinct,
                                   sum as spark_sum)

# ============================================================
#  RUTAS - 4 FUENTES INTEGRADAS (README seccion 4)
# ============================================================
RUTA_ATENCIONES = "data/raw/atenciones_essalud.csv"   # HIS EsSalud
RUTA_EPIDEMIO   = "data/raw/epidemio_minsa_2022_2024.csv"  # MINSA/CDC
RUTA_CLIMA      = "data/raw/clima_lima_2022_2024.csv"      # SENAMHI
RUTA_OFERTA     = "data/raw/oferta_hospitalaria.csv"       # Oferta EsSalud

BRONCE_ATENCIONES = "data/bronze/atenciones"
BRONCE_EPIDEMIO   = "data/bronze/epidemio"
BRONCE_CLIMA      = "data/bronze/clima"
BRONCE_OFERTA     = "data/bronze/oferta"

RUTA_PLATA   = "data/silver/atenciones"
PLATA_OFERTA = "data/silver/oferta"
RUTA_ORO     = "data/gold/demanda_diaria"

ORO_DEMANDA_SEMANAL = "data/gold/demanda_semanal_por_especialidad"
ORO_CORRELACION     = "data/gold/correlacion_clima_enfermedad"
ORO_BRECHAS         = "data/gold/brechas_oferta_demanda_por_establecimiento"


def clave_semana_iso(fecha, num_semana):
    """
    Construye la clave de semana epidemiologica AAAA-Www usando el ANIO ISO 8601,
    no el anio del calendario.

    Motivo (ISO 8601 / semanas epidemiologicas CDC): los primeros dias de enero
    pueden pertenecer a la semana 52 del anio anterior, y los ultimos de diciembre
    a la semana 1 del siguiente. Concatenar year(fecha) con el numero de semana
    genera claves colisionadas: 2022-01-01 y 2022-12-28 recibian ambas la clave
    "2022-W52", lo que volvia el JOIN no determinista.
    """
    anio_iso = (
        when((month(fecha) == 1) & (num_semana >= 52), year(fecha) - 1)
        .when((month(fecha) == 12) & (num_semana == 1), year(fecha) + 1)
        .otherwise(year(fecha))
    )
    return concat(anio_iso.cast("string"), lit("-W"),
                  lpad(num_semana.cast("string"), 2, "0"))


def iniciar_spark():
    return SparkSession.builder \
        .appName("ETL_Medallion_Grupo3") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


# ============================================================
#  CAPA BRONCE
# ============================================================
def capa_bronce(spark):
    """
    CAPA BRONCE: ingesta de las 4 fuentes crudas EXACTAMENTE como llegan.
    No se corrige nada aqui: Bronce es la version auditable del origen.
    """
    print("\n--- CAPA BRONCE: ingestando las 4 fuentes crudas ---")

    fuentes = [
        ("HIS EsSalud (atenciones)", RUTA_ATENCIONES, BRONCE_ATENCIONES),
        ("MINSA/CDC (epidemiologia)", RUTA_EPIDEMIO, BRONCE_EPIDEMIO),
        ("SENAMHI (clima)", RUTA_CLIMA, BRONCE_CLIMA),
        ("EsSalud (oferta hospitalaria)", RUTA_OFERTA, BRONCE_OFERTA),
    ]

    for nombre, origen, destino in fuentes:
        df = spark.read.csv(origen, header=True, inferSchema=True)
        print(f"   {nombre}: {df.count():,} filas")
        df.write.mode("overwrite").parquet(destino)

    print("   -> Bronce guardada en: data/bronze/ (4 fuentes)")


# ============================================================
#  CAPA PLATA - funciones auxiliares
# ============================================================
def _estandarizar_clima(df_clima):
    """
    SENAMHI: recalcula la clave desde la fecha real y agrega las variables
    rezagadas que pide el README (7/14/21 dias). Como la serie es SEMANAL,
    7/14/21 dias equivalen a 1/2/3 semanas de rezago.
    """
    df = (df_clima.withColumn("fecha", to_date(col("fecha")))
                  .withColumn("semana", clave_semana_iso(col("fecha"),
                                                         weekofyear(col("fecha")))))
    w = Window.orderBy("fecha")
    return (df.withColumn("temp_max_lag7",  lag("temperatura_max", 1).over(w))
              .withColumn("temp_max_lag14", lag("temperatura_max", 2).over(w))
              .withColumn("temp_max_lag21", lag("temperatura_max", 3).over(w))
              .withColumn("precip_lag7",    lag("precipitacion_mm", 1).over(w))
              .withColumn("precip_lag14",   lag("precipitacion_mm", 2).over(w))
              .withColumn("precip_lag21",   lag("precipitacion_mm", 3).over(w))
              .select("semana", "temperatura_min",
                      "temp_max_lag7", "temp_max_lag14", "temp_max_lag21",
                      "precip_lag7", "precip_lag14", "precip_lag21"))


def _estandarizar_oferta(df_oferta):
    """
    La oferta NO trae columna 'fecha', solo la clave 'semana' ya mal formada.
    El archivo esta ordenado cronologicamente (verificado en diagnostico):
    cuando una clave aparece dos veces para el mismo establecimiento, la
    PRIMERA corresponde a enero y pertenece al anio anterior.
    """
    df = df_oferta.withColumn("_orden", monotonically_increasing_id())
    w = Window.partitionBy("establecimiento_id", "semana")
    df = (df.withColumn("_n", count("*").over(w))
            .withColumn("_i", row_number().over(w.orderBy("_orden"))))
    df = df.withColumn(
        "semana",
        when((col("_n") > 1) & (col("_i") == 1),
             concat((substring(col("semana"), 1, 4).cast("int") - 1).cast("string"),
                    substring(col("semana"), 5, 4)))
        .otherwise(col("semana"))
    )
    return df.drop("_orden", "_n", "_i")


# ============================================================
#  CAPA PLATA
# ============================================================
def capa_plata(spark):
    """
    CAPA PLATA: limpieza, estandarizacion y JOIN de fuentes (README seccion 4).
    - deduplicacion + limpieza de nulos
    - clave de semana epidemiologica estandarizada (ISO 8601)
    - diagnosticos -> CIE-10 normalizado
    - edad -> grupos etarios (0-5, 6-17, 18-59, 60+)
    - clima -> variables rezagadas 7/14/21 dias
    """
    print("\n--- CAPA PLATA: limpiando, estandarizando e integrando ---")

    df      = spark.read.parquet(BRONCE_ATENCIONES)
    df_epi  = spark.read.parquet(BRONCE_EPIDEMIO)
    df_clima  = spark.read.parquet(BRONCE_CLIMA)
    df_oferta = spark.read.parquet(BRONCE_OFERTA)

    # 1. Limpiar atenciones
    df_limpio = (
        df.dropna(subset=["atencion_id"])
          .dropDuplicates(["atencion_id"])
          .withColumn("especialidad", trim(col("especialidad")))
          .withColumn("establecimiento_nombre", trim(col("establecimiento_nombre")))
    )

    # 2. Clave de semana estandarizada (ISO 8601)
    df_limpio = df_limpio.withColumn(
        "semana",
        clave_semana_iso(col("fecha_atencion"), col("semana_epidemiologica"))
    )

    # 3. CIE-10 normalizado: mayusculas, sin espacios, + capitulo
    df_limpio = (df_limpio
        .withColumn("diagnostico_cie10", upper(trim(col("diagnostico_cie10"))))
        .withColumn("cie10_capitulo", substring(col("diagnostico_cie10"), 1, 1)))

    # 4. Grupos etarios segun README (0-5, 6-17, 18-59, 60+)
    df_limpio = df_limpio.withColumn(
        "grupo_etario",
        when(col("paciente_edad") <= 5, "0-5")
        .when(col("paciente_edad") <= 17, "6-17")
        .when(col("paciente_edad") <= 59, "18-59")
        .otherwise("60+")
    )

    # 5. Epidemiologia MINSA: clave recalculada desde la fecha del boletin
    epi_fix = (df_epi.withColumn("fecha", to_date(col("fecha")))
                     .withColumn("semana", clave_semana_iso(col("fecha"),
                                                            weekofyear(col("fecha")))))

    # 6. Clima SENAMHI: clave recalculada + rezagos 7/14/21 dias
    clima_fix = _estandarizar_clima(df_clima)

    # 7. CONTROL DE CALIDAD: las claves deben ser unicas o el JOIN no es determinista
    for nombre, tabla in [("epidemio", epi_fix), ("clima", clima_fix)]:
        total = tabla.count()
        unicas = tabla.select("semana").distinct().count()
        print(f"   {nombre}: {total} filas / {unicas} claves unicas")
        if total != unicas:
            raise ValueError(f"Claves duplicadas en {nombre}: el JOIN no seria "
                             f"determinista. Revisar data/bronze/{nombre}.")

    # 8. JOIN de fuentes por semana epidemiologica
    epi_sub = epi_fix.select("semana", "casos_dengue", "casos_influenza")
    df_plata = (df_limpio.join(epi_sub, on="semana", how="left")
                         .join(clima_fix, on="semana", how="left"))

    total = df_plata.count()
    nulos = df_plata.filter(col("casos_dengue").isNull()).count()
    print(f"   Registros tras limpieza + JOIN: {total:,}")
    print(f"   Nulos en casos_dengue: {nulos:,} (fuera del rango del boletin MINSA)")

    df_plata.write.mode("overwrite").parquet(RUTA_PLATA)
    print(f"   -> Plata (atenciones) guardada en: {RUTA_PLATA}")

    # 9. Oferta: se estandariza aparte por tener otra granularidad
    #    (semanal por establecimiento, no por atencion)
    oferta_fix = _estandarizar_oferta(df_oferta)
    o_total = oferta_fix.count()
    o_unicas = oferta_fix.select("semana", "establecimiento_id").distinct().count()
    print(f"   oferta: {o_total} filas / {o_unicas} claves unicas")
    if o_total != o_unicas:
        raise ValueError("Claves duplicadas en oferta hospitalaria.")

    oferta_fix.write.mode("overwrite").parquet(PLATA_OFERTA)
    print(f"   -> Plata (oferta) guardada en: {PLATA_OFERTA}")

    return df_plata


# ============================================================
#  CAPA ORO  (se reescribe en el paso siguiente)
# ============================================================
def capa_oro(spark):
    """
    CAPA ORO: tablas analiticas para ML y dashboard (README seccion 4).

    Se excluyen las semanas PARCIALES (menos de 7 dias) de las tablas
    semanales: el dataset arranca el 01/01/2022 y termina el 31/12/2024,
    por lo que 2021-W52 y 2025-W01 solo tienen 2 dias. Incluirlas generaria
    caidas artificiales del ~70% que Isolation Forest marcaria como brotes
    falsos y que distorsionarian la tendencia de Prophet.
    """
    print("\n--- CAPA ORO: construyendo tablas analiticas ---")
    df = spark.read.parquet(RUTA_PLATA)
    oferta = spark.read.parquet(PLATA_OFERTA)

    # --- Identificar semanas completas (7 dias distintos) ---
    dias_por_semana = (df.groupBy("semana")
                         .agg(countDistinct("fecha_atencion").alias("n_dias")))
    semanas_completas = dias_por_semana.filter(col("n_dias") == 7).select("semana")

    parciales = dias_por_semana.filter(col("n_dias") < 7)
    print(f"   Semanas completas: {semanas_completas.count()}")
    print("   Semanas parciales excluidas:")
    parciales.orderBy("semana").show(truncate=False)

    df_sem = df.join(semanas_completas, on="semana", how="inner")

    # ========================================================
    # 1. demanda_semanal_por_especialidad
    # ========================================================
    t1 = (df_sem.groupBy("semana", "especialidad")
          .agg(count("atencion_id").alias("total_atenciones"),
               countDistinct("fecha_atencion").alias("dias_con_atencion"),
               avg("tasa_ocupacion").alias("tasa_ocupacion_promedio"),
               avg("lista_espera_dias").alias("lista_espera_promedio"),
               spark_sum("costo_atencion_soles").alias("costo_total_soles")))
    t1.coalesce(1).write.mode("overwrite").parquet(ORO_DEMANDA_SEMANAL)
    print(f"   [1/4] demanda_semanal_por_especialidad: {t1.count():,} filas")

    # ========================================================
    # 2. correlacion_clima_enfermedad
    # ========================================================
    t2 = (df_sem.groupBy("semana")
          .agg(count("atencion_id").alias("total_atenciones"),
               avg("temperatura_lima_max").alias("temperatura_max"),
               avg("temperatura_min").alias("temperatura_min"),
               avg("precipitacion_mm").alias("precipitacion_mm"),
               avg("indice_humedad").alias("indice_humedad"),
               avg("casos_dengue").alias("casos_dengue"),
               avg("casos_influenza").alias("casos_influenza"),
               avg("temp_max_lag7").alias("temp_max_lag7"),
               avg("temp_max_lag14").alias("temp_max_lag14"),
               avg("temp_max_lag21").alias("temp_max_lag21"),
               avg("precip_lag7").alias("precip_lag7"),
               avg("precip_lag14").alias("precip_lag14"),
               avg("precip_lag21").alias("precip_lag21")))
    t2.coalesce(1).write.mode("overwrite").parquet(ORO_CORRELACION)
    print(f"   [2/4] correlacion_clima_enfermedad: {t2.count():,} filas")

    # ========================================================
    # 3. brechas_oferta_demanda_por_establecimiento
    #    Integra la 4ta fuente (oferta) con la demanda real.
    # ========================================================
    demanda_est = (df_sem.groupBy("semana", "establecimiento_id",
                                  "establecimiento_nombre")
                   .agg(count("atencion_id").alias("demanda_atenciones"),
                        avg("camas_ocupadas_dia").alias("camas_ocupadas_promedio"),
                        avg("tasa_ocupacion").alias("tasa_ocupacion_promedio")))

    t3 = (demanda_est.join(oferta, on=["semana", "establecimiento_id"], how="inner")
          .withColumn("brecha_consultas",
                      col("demanda_atenciones") - col("consultas_programadas"))
          .withColumn("brecha_camas",
                      col("camas_disponibles") - col("camas_ocupadas_promedio"))
          .withColumn("ratio_demanda_oferta",
                      col("demanda_atenciones") / col("consultas_programadas")))
    t3.coalesce(1).write.mode("overwrite").parquet(ORO_BRECHAS)
    print(f"   [3/4] brechas_oferta_demanda_por_establecimiento: {t3.count():,} filas")

    # ========================================================
    # 4. demanda_diaria (serie para Prophet - conserva todos los dias)
    # ========================================================
    t4 = (df.groupBy("fecha_atencion", "establecimiento_nombre", "especialidad")
          .agg(count("atencion_id").alias("total_pacientes_diarios"),
               avg("temperatura_lima_max").alias("temperatura_promedio"),
               avg("casos_dengue").alias("casos_dengue_minsa"),
               avg("tasa_ocupacion").alias("tasa_ocupacion_promedio")))
    t4.coalesce(1).write.mode("overwrite").parquet(RUTA_ORO)
    print(f"   [4/4] demanda_diaria: {t4.count():,} filas")

    print("   -> Oro guardada en: data/gold/")
    return t1


def procesar_etl():
    spark = iniciar_spark()
    try:
        capa_bronce(spark)
        capa_plata(spark)
        capa_oro(spark)
        print("\n\u00a1EXITO! Las 3 capas (Bronce, Plata, Oro) se generaron correctamente.")
    finally:
        spark.stop()


if __name__ == "__main__":
    procesar_etl()