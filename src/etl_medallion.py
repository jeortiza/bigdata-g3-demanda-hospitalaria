from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim

def iniciar_spark():
    print("Iniciando sesión de PySpark...")
    spark = SparkSession.builder \
        .appName("ETL_Medallion_EsSalud") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    return spark

def ejecutar_capa_bronce(spark):
    print("\n--- Ejecutando Capa Bronce (Datos Crudos) ---")
    ruta_raw = "data/raw/atenciones_essalud.csv"
    df_bronce = spark.read.csv(ruta_raw, header=True, inferSchema=True)
    print(f"Registros en Capa Bronce: {df_bronce.count()}")
    return df_bronce

def ejecutar_capa_plata(df_bronce):
    print("\n--- Ejecutando Capa Plata (Limpieza y Estandarización) ---")
    # 1. Eliminar duplicados exactos
    df_plata = df_bronce.dropDuplicates()
    
    # 2. Eliminar filas donde el ID o la fecha sean nulos (datos corruptos)
    df_plata = df_plata.dropna(subset=["atencion_id", "fecha_atencion"])
    
    # 3. Limpiar espacios en blanco al inicio o final de los textos
    df_plata = df_plata.withColumn("especialidad", trim(col("especialidad"))) \
                       .withColumn("establecimiento_nombre", trim(col("establecimiento_nombre")))
    
    print(f"Registros limpios en Capa Plata: {df_plata.count()}")
    return df_plata

def ejecutar_capa_oro(df_plata):
    print("\n--- Ejecutando Capa Oro (Agregación para Machine Learning) ---")
    # Para predecir la demanda, necesitamos saber cuántos pacientes llegan por día, por hospital y por especialidad
    df_oro = df_plata.groupBy("fecha_atencion", "establecimiento_nombre", "especialidad") \
                     .agg(count("atencion_id").alias("total_pacientes_diarios")) \
                     .orderBy("fecha_atencion")
    
    print("Muestra de los primeros 5 registros listos para Machine Learning (Capa Oro):")
    df_oro.show(5)
    print(f"Total de registros agrupados en Capa Oro: {df_oro.count()}")
    return df_oro

if __name__ == "__main__":
    # 1. Encender el motor
    spark = iniciar_spark()
    
    # 2. Ejecutar el flujo de la Arquitectura Medallion completo
    df_bronce = ejecutar_capa_bronce(spark)
    df_plata = ejecutar_capa_plata(df_bronce)
    df_oro = ejecutar_capa_oro(df_plata)
    
    print("\n¡ETL Medallion completado con éxito en sus 3 capas!")
    
    # 3. Apagar el motor
    spark.stop()