# Diccionario de Datos — Sistema HIS EsSalud (simulado)

## Tabla: atenciones_essalud

| Campo | Tipo | Valores posibles | Descripción |
|-------|------|-----------------|-------------|
| atencion_id | STRING | TKT-YYYY-NNNNNN | ID único de atención |
| fecha_atencion | DATE | 2022-01-01 a 2024-12-31 | Fecha de la atención |
| semana_epidemiologica | INT | 1-52 | Semana CDC correspondiente |
| establecimiento_id | STRING | HOSP-001 a POLI-010 | Código del establecimiento |
| tipo_atencion | STRING | consulta_externa, emergencia, hospitalizacion | Modalidad de atención |
| especialidad | STRING | 25 especialidades | Servicio médico |
| diagnostico_cie10 | STRING | Código A00-Z99 | Diagnóstico principal CIE-10 |
| dias_estancia | INT | 0-90 | Días hospitalizados (0 si ambulatorio) |
| tasa_ocupacion | FLOAT | 0.0-1.5 | Camas ocupadas / disponibles |
| sla_cumplido | BOOL | True/False | Atendido en tiempo objetivo |
| temperatura_lima_max | FLOAT | 15-32°C | Temperatura máxima Lima ese día |
| casos_dengue_semana | INT | 0-5000 | Casos dengue Lima esa semana |
| alerta_epidemiologica | BOOL | True/False | Alerta MINSA vigente |

## Tabla: oferta_hospitalaria

| Campo | Tipo | Descripción |
|-------|------|-------------|
| semana | STRING | YYYY-WNN (semana ISO) |
| establecimiento_id | STRING | Código del establecimiento |
| camas_totales | INT | Dotación total de camas |
| camas_disponibles | INT | Camas operativas ese período |
| medicos_activos | INT | Médicos en guardia/consultorio |
| productividad_medica | FLOAT | Consultas/médico/semana |
| lista_espera_dias | INT | Promedio días para primera cita |
