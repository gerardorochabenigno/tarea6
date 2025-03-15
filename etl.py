import requests
import argparse
import pandas as pd
import yaml
import numpy as np
import os
import logging
from source.etl import series_sie_completa

# Variables del config
with open("config/config.yaml", "r") as file:
    config = yaml.safe_load(file)

LOG_PATH = config['log_etl'] 
TOKEN_SIE = config['token_sie']

# Configuración del log
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Obtener series de tiempo del Banco de México para tipo de cambio, tasa de interés e inflación")
    parser.add_argument(
        "--serie",
        type=str,
        choices=['tipo_de_cambio', 'tasa_de_interes', 'inflacion'],
        required=True,
        help="Serie a obtener: 'tipo_de_cambio', 'tasa_de_interes' o 'inflacion'"
    )

    args = parser.parse_args()

    try:
        df = series_sie_completa(TOKEN_SIE, args.serie)

        # Crear la carpeta data/raw si no existe
        os.makedirs("data/raw", exist_ok=True)

        # Guardar el archivo CSV
        nombre_serie = os.path.join('data/raw', args.serie) + '.csv'
        df.to_csv(nombre_serie, index=False)

        logging.info(f"ETL exitoso. Datos guardados en {nombre_serie}")
        print(f"ETL completado con éxito. Archivo guardado en {nombre_serie}")

    except Exception as e:
        logging.error(f"Error en el ETL para {args.serie}: {e}")
        print("Error. Revisa el log.")