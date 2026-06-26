import os
import sys

# --- Configuración Hadoop/winutils para Windows ---
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\hadoop\bin"
# ---------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim

# ============================================================
#  RUTAS DE CADA CAPA DE LA ARQUITECTURA MEDALLION
#  Cada capa se persiste en su propia carpeta para que el
#  flujo Bronce -> Plata -> Oro sea trazable (Grupo 3).
# ============================================================
RUTA_RAW    = "data/raw/atenciones_essalud.csv"  # fuente cruda (HIS EsSalud)
RUTA_BRONCE = "data/bronze/atenciones"           # datos tal como llegan
RUTA_PLATA  = "data/silver/atenciones"           # datos limpios y estandarizados
RUTA_ORO    = "data/gold/demanda_diaria"         # tabla analítica para KPIs / dashboard


def iniciar_spark():
    # Configuración ajustada para una PC local
    return SparkSession.builder \
        .appName("ETL_Medallion_Grupo3") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


def capa_bronce(spark):
    """
    CAPA BRONCE: ingesta de datos crudos EXACTAMENTE como llegan.
    No se transforma nada; solo se lee el CSV y se persiste en Parquet.
    """
    print("\n--- CAPA BRONCE: leyendo datos crudos ---")
    df_bronce = spark.read.csv(RUTA_RAW, header=True, inferSchema=True)
    print(f"   Registros crudos leidos: {df_bronce.count():,}")

    df_bronce.write.mode("overwrite").parquet(RUTA_BRONCE)
    print(f"   -> Bronce guardada en: {RUTA_BRONCE}")
    return df_bronce


def capa_plata(spark):
    """
    CAPA PLATA: limpieza y estandarizacion a partir de la capa Bronce.
    - Elimina registros sin clave (atencion_id)
    - Elimina duplicados (re-consultas)
    - Normaliza espacios en campos de texto
    """
    print("\n--- CAPA PLATA: limpiando y estandarizando ---")
    df = spark.read.parquet(RUTA_BRONCE)

    df_plata = (
        df.dropna(subset=["atencion_id"])
          .dropDuplicates(["atencion_id"])
          .withColumn("especialidad", trim(col("especialidad")))
          .withColumn("establecimiento_nombre", trim(col("establecimiento_nombre")))
    )
    print(f"   Registros tras limpieza: {df_plata.count():,}")

    df_plata.write.mode("overwrite").parquet(RUTA_PLATA)
    print(f"   -> Plata guardada en: {RUTA_PLATA}")
    return df_plata


def capa_oro(spark):
    """
    CAPA ORO: tabla analitica agregada a partir de la capa Plata.
    Demanda diaria de pacientes por establecimiento y especialidad
    (base para los KPIs y el dashboard gerencial).
    """
    print("\n--- CAPA ORO: agregando para analisis ---")
    df = spark.read.parquet(RUTA_PLATA)

    df_oro = (
        df.groupBy("fecha_atencion", "establecimiento_nombre", "especialidad")
          .agg(count("atencion_id").alias("total_pacientes_diarios"))
    )

    # coalesce(1) -> un solo archivo de salida, comodo para inspeccionar
    df_oro.coalesce(1).write.mode("overwrite").parquet(RUTA_ORO)
    print(f"   -> Oro guardada en: {RUTA_ORO}")
    return df_oro


def procesar_etl():
    spark = iniciar_spark()
    try:
        capa_bronce(spark)   # 1. Bronce
        capa_plata(spark)    # 2. Plata
        capa_oro(spark)      # 3. Oro
        print("\n¡EXITO! Las 3 capas (Bronce, Plata, Oro) se generaron correctamente.")
        print("   data/bronze/ | data/silver/ | data/gold/")
    finally:
        spark.stop()


if __name__ == "__main__":
    procesar_etl()