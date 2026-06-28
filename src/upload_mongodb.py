import pandas as pd
from pymongo import MongoClient

# 1. Configurar la conexión (¡AQUÍ PEGARÁS TU LLAVE MAESTRA!)
MONGO_URI = "mongodb+srv://jortiz260590_db_user:2iWMA7mX9UVjA2rv@cluster0.eznmg3v.mongodb.net/?appName=Cluster0"

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