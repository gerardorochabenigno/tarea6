import requests
import argparse
import pandas as pd
import yaml
import numpy as np
import os

SERIE = "SF43718"
FECHA_INICIO = '2000-01-01'
FECHA_FIN = '2025-03-10'

with open("config/config.yaml", "r") as file:
    config = yaml.safe_load(file)
TOKEN_SIE = config['token_sie']

def series_sie_completa(token:str, serie:str) -> pd.DataFrame:
    """
    Función para obtener series de tiempo de tipo de cambio, tasa de interés e inflación. Información proveniente del Sistema de Información Económica (SIE) de Banco de México.
    Args:
        token: Token de Banco de México
        serie: Identificador de la serie. Solo tres series son permitidas
            'tipo_de_cambio':  'SF43718', Tipo de cambio FIX determinado por el Banco de México con base en un promedio de cotizaciones del mercado de cambios al mayoreo para operaciones liquidables el segundo día hábil bancario siguiente.
            'tasa_de_interes': 'SF43783', TIIE 28 aplicable en la fecha correspondiente (fecha de publicación en el Diario Oficial de la Federación) y determinada por el Banco de México.
            'inflacion':       'SP74833', Variación anual del INPC. El INPC lo construye el INEGI, pero Banco de México también publica esta información en el SIE.
    Returns:
        pd.Dataframe: pandas dataframe con 

    """
    if serie in ['tipo_de_cambio','tasa_de_interes','inflacion']:
        idserie = {'tipo_de_cambio':'SF43718', 'tasa_de_interes':'SF43783', 'inflacion':'SP74833'}
    else:
        raise Exception("Solo las series 'SF43718', 'SF43783', 'SP74833' son permitidas.")

    
    try:
        url = f'https://www.banxico.org.mx/SieAPIRest/service/v1/series/{idserie[serie]}/datos'
        # Necesario para realizar la consulta
        headers = {"Bmx-Token": token}
        # Hacemos la llamada a la API
        response = requests.get(url, headers=headers)
        # Obtenemos el JSON
        data = response.json()
        # Lista de diccionarios
        series = data['bmx']['series'][0]['datos']
        # Pasamos a dataframe
        df = pd.DataFrame(series)
        # Tratamos columna de fecha
        df['fecha'] = pd.to_datetime(df['fecha'], format="%d/%m/%Y")
        # Tratamos columna de dato
        valores_invalidos = ['NA', 'NE', 'No aplica', 'Error', 'N/A', 'nan', 'N/E']
        df['dato'] = df['dato'].replace(valores_invalidos, np.nan)
        df['dato'] = df['dato'].astype(float)
        df = df.rename(columns={'dato':serie})
        return df
    
    except:
        print("La solicitud no pudo ser procesada")




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
    df = series_sie_completa(TOKEN_SIE, args.serie)
    nombre_serie = os.path.join('data/raw', args.serie)+'.csv'
    df.to_csv(nombre_serie, index = False)

    
    