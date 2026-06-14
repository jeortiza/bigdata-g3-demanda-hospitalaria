# Datos del Proyecto — Predicción Demanda Hospitalaria EsSalud

## Cómo generar el dataset completo

```bash
# Genera 500,000 atenciones sintéticas (3 años: 2022-2024)
python src/generador_atenciones.py
# Output:
#   data/raw/atenciones_essalud.csv         (~280MB)
#   data/raw/clima_lima_2022_2024.csv       (~1MB, SENAMHI simulado)
#   data/raw/epidemio_minsa_2022_2024.csv   (~500KB, boletines epidemiológicos)
#   data/raw/oferta_hospitalaria.csv        (~200KB, oferta semanal por establecimiento)
```

## Fuentes externas para enriquecer el dataset
- SENAMHI: https://www.senamhi.gob.pe/?&p=data-historica
- MINSA Epidemiología: https://www.dge.gob.pe/sala-situacional-dengue/
- SuSalud: https://www.susalud.gob.pe/
- data.gob.pe MINSA: https://www.datosabiertos.gob.pe/organization/ministerio-de-salud-minsa

## Archivos de muestra (en GitHub)
- `data/sample/atenciones_muestra_200.csv` — 200 registros de ejemplo
- `data/sample/cie10_codigos.json` — catálogo CIE-10 principales
- `data/sample/establecimientos_essalud.json` — 20 establecimientos Lima
- `data/sample/semanas_epidemiologicas.csv` — calendario epidemiológico 2022-2024

## Esquema de campos principales
Ver `docs/diccionario_datos_his.md` para la descripción completa de cada campo.
