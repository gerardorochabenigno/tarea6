# etl.py

import pandas as pd
import requests
import numpy as np
import logging
import boto3
import os

def series_sie_completa(token: str, serie: str) -> pd.DataFrame:
    """
    Obtiene series de tiempo del SIE de Banco de México y guarda errores en un log configurado en `config.yaml`.

    Args:
        token (str): Token de acceso a la API del Banco de México.
        serie (str): Identificador de la serie ('tipo_de_cambio', 'tasa_de_interes', 'inflacion').

    Returns:
        pd.DataFrame: DataFrame con las series de tiempo procesadas.
    """
    if serie not in ['tipo_de_cambio', 'tasa_de_interes', 'inflacion']:
        error_msg = f"Serie '{serie}' no permitida. Solo 'tipo_de_cambio', 'tasa_de_interes', 'inflacion'."
        logging.error(error_msg)
        raise ValueError(error_msg)

    idserie = {'tipo_de_cambio': 'SF43718', 'tasa_de_interes': 'SF43783', 'inflacion': 'SP1'}

    try:
        # Para el GET
        url = f'https://www.banxico.org.mx/SieAPIRest/service/v1/series/{idserie[serie]}/datos'
        headers = {"Bmx-Token": token}

        #GET
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Datos
        data = response.json()
        series = data['bmx']['series'][0]['datos']
        df = pd.DataFrame(series)

        # Tratamiento de fecha
        df['fecha'] = pd.to_datetime(df['fecha'], format="%d/%m/%Y")

        # Tratamiento de valor
        valores_invalidos = ['NA', 'NE', 'No aplica', 'Error', 'N/A', 'nan', 'N/E']
        df['dato'] = df['dato'].replace(valores_invalidos, np.nan).astype(float)
        df = df.rename(columns={'dato': serie})

        if serie == 'inflacion':
            df['inpc_lag_12'] = df['inflacion'].shift(12)
            df['inflacion'] = 100 * (df['inflacion'] / df['inpc_lag_12'] - 1)
            df = df[['fecha', serie]]

        # No ocupamos valores nulos
        df = df.dropna()

        return df

    except requests.exceptions.RequestException as e:
        error_msg = f"Error al obtener datos de {serie}: {e}"
        logging.error(error_msg)

    except ValueError as e:
        error_msg = f"Error en los datos de {serie}: {e}"
        logging.error(error_msg)

    except Exception as e:
        error_msg = f"Error inesperado en {serie}: {e}"
        logging.error(error_msg)

    return pd.DataFrame()


def subir_a_s3(ruta_local: str, ruta_s3: str, bucket_name: str = 'itam-analytics-grb', profile_name: str = None) -> None:
    """
    Sube un archivo a S3.

    Args:
        ruta_local (str): Ruta local del archivo.
        ruta_s3 (str): Ruta dentro del bucket de S3.
        bucket_name (str, opcional): Nombre del bucket de S3. Default: 'itam-analytics-grb'.
        profile_name (str, opcional): Perfil de AWS a usar. Si es None, usa el perfil por defecto.

    Returns:
        None
    """
    try:
        # Verificar si el archivo local existe
        if not os.path.exists(ruta_local):
            raise FileNotFoundError(f"El archivo {ruta_local} no existe.")

        # Crear sesión de boto3 con perfil opcional
        if profile_name:
            session = boto3.Session(profile_name=profile_name)
            s3 = session.client("s3")
        else:
            s3 = boto3.client("s3")

        # Subir archivo a S3
        s3.upload_file(ruta_local, bucket_name, ruta_s3)
        logging.info(f"Archivo {ruta_local} subido exitosamente a s3://{bucket_name}/{ruta_s3}")
        print(f"Archivo subido exitosamente a s3://{bucket_name}/{ruta_s3}")

    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        print(f"Error: {e}")

    except Exception as e:
        logging.error(f"Error al subir {ruta_local} a S3: {e}")
        print(f"Error al subir {ruta_local} a S3: {e}")