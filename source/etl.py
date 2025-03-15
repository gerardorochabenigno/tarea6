# etl.py

import pandas as pd
import requests
import numpy as np
import logging

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
