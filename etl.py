import requests
import argparse
import pandas as pd
import yaml
import numpy as np
import os
import logging
from source.etl import series_sie_completa
from source.etl import subir_a_s3
import boto3

# Variables del config
with open("config/config.yaml", "r") as file:
    config = yaml.safe_load(file)

LOG_PATH = config['log_etl'] 
TOKEN_SIE = config['token_sie']
DEFAULT_BUCKET = config['default_bucket']

# Configuración del log
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Obtener series de tiempo del Banco de México y subir a S3")
    parser.add_argument(
        "--serie",
        type=str,
        choices=['tipo_de_cambio', 'tasa_de_interes', 'inflacion'],
        required=True,
        help="Serie a obtener: 'tipo_de_cambio', 'tasa_de_interes' o 'inflacion'"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=False,
        default=DEFAULT_BUCKET,
        help=f"Nombre del bucket de S3. Default: {DEFAULT_BUCKET}"
    )
    parser.add_argument(
        "--profile",
        type=str,
        required=True,
        help="Nombre del perfil de AWS"
    )

    args = parser.parse_args()

    try:
        # Obtener los datos del SIE
        df = series_sie_completa(TOKEN_SIE, args.serie)

        # Guardar en local
        os.makedirs("data/raw", exist_ok=True)
        ruta_local = os.path.join('data/raw', f"{args.serie}.csv")
        df.to_csv(ruta_local, index=False)

        logging.info(f"ETL exitoso. Datos guardados en {ruta_local}")
        print(f"Archivo guardado en {ruta_local}")

        # Subir a S3
        ruta_s3 = f"econ/raw/{args.serie}/{args.serie}.csv"
        subir_a_s3(ruta_local, ruta_s3, bucket_name=args.bucket, profile_name=args.profile)
        logging.info(f"Archivo {ruta_local} subido exitosamente a s3://{args.bucket}/{ruta_s3}")

    except Exception as e:
        logging.error(f"Error en el ETL para {args.serie}: {e}")
        print(f"Error en el ETL. Revisa el log.")