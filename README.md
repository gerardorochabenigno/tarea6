# Tarea 6: Métodos de Gran Escala.

## Objetivo

Crear un ETL y un ELT para la automatización del análisis del tipo de cambio, tasa de interés e inflación utilizando Amazon S3, Amazon Glue y Amazon Athena y desplegando la aplicación en streamlit. 

Para utilizar el programa se puede hacer de la siguiente forma:
 - 1. Correr el script etl.py para extraer los datos de la API de Banxico y guardarlos en un bucket de S3.
 - 2. Correr el script elt.py actualizar la base de datos "econ" almacenada en Glue.
 - 3. Correr el script app.py para desplegar la aplicación en streamlit.

La estructura de la carpeta es la siguiente:

```
Tarea6/
|__ config/
|____ config.yaml
|__ src/
|____ etl.py
|____ elt.py
|__ streamlit/
|____ app.py
|__ data/
|____ raw/
|__ logs/
|____ etl.log
|____ elt.log
|__ notebooks/
|____ etl.ipynb
|____ elt.ipynb
|____ query.ipynb

```
