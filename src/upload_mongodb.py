import pandas as pd
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

# 1. Leer la conexión desde el .env (ya no está escrita en el código)
MONGO_URI = os.getenv("MONGO_URI")

def subir_a_mongodb():
    print("Iniciando proceso de carga a MongoDB Atlas...")
    
    # 2. Conectarse a MongoDB
    try:
        cliente = MongoClient(MONGO_URI)
        db = cliente["bigdata_g3"] # Se creará la base de datos de tu grupo
        coleccion = db["demanda_diaria"] # Se creará la tabla (colección)
        
        # 3. Leer los datos de la Capa Oro (formato Parquet)
        ruta_oro = "data/gold/demanda_diaria"
        print(f"Leyendo archivos Parquet desde: {ruta_oro}")
        df_oro = pd.read_parquet(ruta_oro)
        
        # 4. Convertir los datos a formato diccionario (JSON) que usa MongoDB
        registros = df_oro.to_dict("records")
        
        # 5. Insertar en MongoDB
        print(f"Subiendo {len(registros)} registros a la nube... (esto puede tardar unos segundos)")
        
        # Borramos datos anteriores por si ejecutamos el script más de una vez
        coleccion.delete_many({}) 
        coleccion.insert_many(registros)
        
        print("¡ÉXITO! Todos los datos fueron subidos a MongoDB Atlas correctamente.")
        
    except Exception as e:
        print(f"Ocurrió un error al intentar subir los datos: {e}")

if __name__ == "__main__":
    subir_a_mongodb()