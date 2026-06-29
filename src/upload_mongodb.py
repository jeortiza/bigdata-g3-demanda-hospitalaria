import pandas as pd
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

# 1. Leer la conexion desde el .env (ya no esta escrita en el codigo)
MONGO_URI = os.getenv("MONGO_URI")

def subir_a_mongodb():
    print("Iniciando proceso de carga a MongoDB Atlas...")

    # 2. Conectarse a MongoDB
    try:
        cliente = MongoClient(MONGO_URI)
        db = cliente["bigdata_g3"]            # base de datos del grupo
        coleccion = db["demanda_diaria"]      # coleccion (tabla)

        # 3. Leer los datos de la Capa Oro (formato Parquet)
        ruta_oro = "data/gold/demanda_diaria"
        print(f"Leyendo archivos Parquet desde: {ruta_oro}")
        df_oro = pd.read_parquet(ruta_oro)

        # 3.1 Convertir columnas de fecha/datetime a texto
        #     (MongoDB no puede guardar el tipo datetime.date directamente)
        for columna in df_oro.columns:
            tipo = str(df_oro[columna].dtype).lower()
            if "date" in tipo or "datetime" in tipo or df_oro[columna].dtype == "object":
                df_oro[columna] = df_oro[columna].astype(str)

        # 4. Convertir los datos a formato diccionario (JSON) que usa MongoDB
        registros = df_oro.to_dict("records")

        # 5. Insertar en MongoDB
        print(f"Subiendo {len(registros)} registros a la nube... (esto puede tardar unos segundos)")

        # Borramos datos anteriores por si ejecutamos el script mas de una vez
        coleccion.delete_many({})
        coleccion.insert_many(registros)

        print("\u00a1EXITO! Todos los datos fueron subidos a MongoDB Atlas correctamente.")

    except Exception as e:
        print(f"Ocurrio un error al intentar subir los datos: {e}")

if __name__ == "__main__":
    subir_a_mongodb()