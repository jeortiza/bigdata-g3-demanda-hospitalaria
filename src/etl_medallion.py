import os
import sys

# --- Configuracion Hadoop/winutils para Windows ---
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\hadoop\bin"
# ---------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, trim, concat, lit, lpad, avg,
                                   month, year, when, to_date, weekofyear)

# ============================================================
#  RUTAS DE CADA CAPA DE LA ARQUITECTURA MEDALLION (Grupo 3)
# ============================================================
RUTA_ATENCIONES = "data/raw/atenciones_essalud.csv"
RUTA_EPIDEMIO   = "data/raw/epidemio_minsa_2022_2024.csv"

RUTA_BRONCE = "data/bronze/atenciones"
RUTA_PLATA  = "data/silver/atenciones"
RUTA_ORO    = "data/gold/demanda_diaria"

def clave_semana_iso(fecha, num_semana):
    """
    Construye la clave de semana epidemiologica AAAA-Www usando el ANIO ISO 8601,
    no el anio del calendario.

    Motivo (norma ISO 8601 / semanas epidemiologicas CDC): los primeros dias de
    enero pueden pertenecer a la semana 52 del anio anterior, y los ultimos de
    diciembre a la semana 1 del siguiente. Concatenar year(fecha) con el numero
    de semana genera claves colisionadas: 2022-01-01 y 2022-12-28 recibian ambas
    la clave "2022-W52", lo que volvia el JOIN no determinista.
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


def capa_bronce(spark):
    """
    CAPA BRONCE: ingesta de las fuentes crudas EXACTAMENTE como llegan.
    Nota: las atenciones YA incluyen temperatura y precipitacion (del generador),
    asi que del epidemiologico solo necesitamos los casos de dengue/influenza.
    """
    print("\n--- CAPA BRONCE: leyendo las fuentes crudas ---")
    df_aten = spark.read.csv(RUTA_ATENCIONES, header=True, inferSchema=True)
    df_epi = spark.read.csv(RUTA_EPIDEMIO, header=True, inferSchema=True)

    print(f"   Atenciones crudas: {df_aten.count():,}")
    print(f"   Epidemio (semanas):{df_epi.count()}")

    df_aten.write.mode("overwrite").parquet(RUTA_BRONCE)
    df_epi.write.mode("overwrite").parquet("data/bronze/epidemio")
    print(f"   -> Bronce guardada en: data/bronze/")
    return df_aten, df_epi


def capa_plata(spark):
    """
    CAPA PLATA: limpieza, estandarizacion y JOIN de fuentes.
    - Limpia atenciones (sin nulos, sin duplicados)
    - Estandariza la clave de semana epidemiologica (ISO 8601)
    - Integra epidemiologia (dengue / influenza MINSA) por semana
    """
    print("\n--- CAPA PLATA: limpiando e integrando fuentes (JOIN) ---")
    df = spark.read.parquet(RUTA_BRONCE)
    df_epi = spark.read.parquet("data/bronze/epidemio")

    # 1. Limpiar atenciones
    df_limpio = (
        df.dropna(subset=["atencion_id"])
          .dropDuplicates(["atencion_id"])
          .withColumn("especialidad", trim(col("especialidad")))
          .withColumn("establecimiento_nombre", trim(col("establecimiento_nombre")))
    )

    # 2. Estandarizar la clave de semana (ISO 8601)
    df_limpio = df_limpio.withColumn(
        "semana",
        clave_semana_iso(col("fecha_atencion"), col("semana_epidemiologica"))
    )

    # 3. Epidemiologia: recalcular la clave desde la FECHA real del boletin
    epi_fix = (
        df_epi.withColumn("fecha", to_date(col("fecha")))
              .withColumn("semana", clave_semana_iso(col("fecha"),
                                                     weekofyear(col("fecha"))))
    )

    # 3b. CONTROL DE CALIDAD: la clave debe ser unica o el JOIN no es determinista
    total_epi = epi_fix.count()
    claves_epi = epi_fix.select("semana").distinct().count()
    print(f"   Epidemio: {total_epi} filas / {claves_epi} claves unicas")
    if total_epi != claves_epi:
        raise ValueError(
            "Claves de semana duplicadas en epidemiologia: el JOIN no seria "
            "determinista. Revisar data/bronze/epidemio."
        )

    epi_sub = epi_fix.select("semana", "casos_dengue", "casos_influenza")

    # 4. JOIN: atenciones + casos MINSA por semana epidemiologica
    df_plata = df_limpio.join(epi_sub, on="semana", how="left")

    total = df_plata.count()
    nulos = df_plata.filter(col("casos_dengue").isNull()).count()
    print(f"   Registros tras limpieza + JOIN: {total:,}")
    print(f"   Nulos en casos_dengue: {nulos:,} "
          f"(semanas fuera del rango del boletin MINSA)")
    print("   Fuentes integradas: atenciones (con clima) + epidemiologia (MINSA)")

    df_plata.write.mode("overwrite").parquet(RUTA_PLATA)
    print(f"   -> Plata guardada en: {RUTA_PLATA}")
    return df_plata


def capa_oro(spark):
    """
    CAPA ORO: tabla analitica agregada para KPIs y dashboard.
    Demanda diaria por establecimiento y especialidad, ENRIQUECIDA
    con temperatura (de atenciones) y casos de dengue (de epidemiologia).
    """
    print("\n--- CAPA ORO: agregando para analisis (con clima y dengue) ---")
    df = spark.read.parquet(RUTA_PLATA)

    df_oro = (
        df.groupBy("fecha_atencion", "establecimiento_nombre", "especialidad")
          .agg(
              count("atencion_id").alias("total_pacientes_diarios"),
              avg("temperatura_lima_max").alias("temperatura_promedio"),
              avg("casos_dengue").alias("casos_dengue_semana"),
              avg("tasa_ocupacion").alias("tasa_ocupacion_promedio")
          )
    )

    df_oro.coalesce(1).write.mode("overwrite").parquet(RUTA_ORO)
    print(f"   -> Oro guardada en: {RUTA_ORO}")
    return df_oro


def procesar_etl():
    spark = iniciar_spark()
    try:
        capa_bronce(spark)
        capa_plata(spark)
        capa_oro(spark)
        print("\n\u00a1EXITO! Las 3 capas (Bronce, Plata, Oro) se generaron correctamente.")
        print("   La capa Oro ahora integra: demanda + clima + dengue")
    finally:
        spark.stop()


if __name__ == "__main__":
    procesar_etl()