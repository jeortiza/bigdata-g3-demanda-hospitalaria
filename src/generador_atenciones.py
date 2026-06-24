import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

# Inicializar Faker para Perú
fake = Faker('es_MX')

# Catálogo CIE-10 (De la documentación del proyecto)
DIAGNOSTICOS = {
    "A90": ("Dengue (sin signo de alarma)", "Enfermedades_infecciosas"),
    "J11": ("Influenza", "Enfermedades_infecciosas"),
    "J06": ("Infección aguda vías respiratorias", "Respiratorias"),
    "I10": ("Hipertensión esencial", "Cardiovascular"),
    "E11": ("Diabetes mellitus tipo 2", "Crónicas"),
    "S00-S99": ("Traumatismos", "Traumatología")
}
ESPECIALIDADES = ["Medicina Interna", "Emergencia", "Pediatría", "Cardiología", "Traumatología"]
ESTABLECIMIENTOS = ["Hospital Almenara", "Hospital Rebagliati", "Hospital Sabogal"]

def generar_datos(num_registros=500000):
    print(f"Generando {num_registros} registros sintéticos. Esto puede tomar un momento...")
    datos = []
    
    fecha_inicio = datetime(2022, 1, 1)
    fecha_fin = datetime(2024, 12, 31)
    
    for i in range(num_registros):
        cie10 = random.choice(list(DIAGNOSTICOS.keys()))
        nombre_diag, grupo_diag = DIAGNOSTICOS[cie10]
        
        # Generar fecha aleatoria en los últimos 3 años
        dias_diferencia = (fecha_fin - fecha_inicio).days
        fecha_atencion = fecha_inicio + timedelta(days=random.randint(0, dias_diferencia))
        
        registro = {
            "atencion_id": f"ATN-{fecha_atencion.year}-{str(i).zfill(7)}",
            "fecha_atencion": fecha_atencion.strftime("%Y-%m-%d"),
            "establecimiento_nombre": random.choice(ESTABLECIMIENTOS),
            "especialidad": random.choice(ESPECIALIDADES),
            "paciente_edad": random.randint(1, 90),
            "paciente_sexo": random.choice(["M", "F"]),
            "diagnostico_cie10": cie10,
            "diagnostico_nombre": nombre_diag,
            "diagnostico_grupo": grupo_diag,
            "tasa_ocupacion": round(random.uniform(0.60, 1.20), 2)
        }
        datos.append(registro)
        
        if (i + 1) % 100000 == 0:
            print(f"... {i + 1} registros generados")

    df = pd.DataFrame(datos)
    
    # Crear carpeta si no existe y guardar
    os.makedirs('data/raw', exist_ok=True)
    ruta_salida = 'data/raw/atenciones_essalud.csv'
    df.to_csv(ruta_salida, index=False)
    print(f"¡Éxito! Dataset guardado en: {ruta_salida}")

if __name__ == "__main__":
    # Generamos los 500k registros (puedes bajar el número a 1000 para una prueba rápida)
    generar_datos(500000)