import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.vector_ar.vecm import VECM, coint_johansen
import boto3
import awswrangler as wr
import os
import yaml

# Configuración de la página
st.set_page_config(page_title="Análisis de VECM", layout="wide")

# Variables del config
parent_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
with open(os.path.join(parent_dir, "config/config.yaml"), "r") as file:
    config = yaml.safe_load(file)
PROFILE = config['profile']

# Configuración del cliente
session = boto3.Session(profile_name=PROFILE)
s3 = session.client('s3')

# Cargamos los datos
query = """
WITH 
    tipo_de_cambio_mensual AS (
        SELECT 
            DATE_TRUNC('month', fecha) AS fecha,
            AVG(tipo_de_cambio) AS tipo_de_cambio_mensual
        FROM econ.tipo_de_cambio
        GROUP BY 1
    ),

    tasa_de_interes_mensual AS (
        SELECT 
            DATE_TRUNC('month', fecha) AS fecha,
            AVG(tasa_de_interes) AS tasa_de_interes_mensual
        FROM econ.tasa_de_interes
        GROUP BY 1
    ),

    inflacion_mensual AS (
        SELECT 
            DATE_TRUNC('month', fecha) AS fecha,
            AVG(inflacion) AS inflacion_mensual
        FROM econ.inflacion
        GROUP BY 1
    )

SELECT 
    tdc.fecha,
    tdc.tipo_de_cambio_mensual,
    ti.tasa_de_interes_mensual,
    inf.inflacion_mensual
FROM tipo_de_cambio_mensual tdc
LEFT JOIN tasa_de_interes_mensual ti ON tdc.fecha = ti.fecha
LEFT JOIN inflacion_mensual inf ON tdc.fecha = inf.fecha
ORDER BY tdc.fecha;

"""

series = wr.athena.read_sql_query(
    
    query, 
    database="econ", 
    ctas_approach=False, 
    boto3_session=session
    
    )

series['fecha'] = pd.to_datetime(series['fecha'])
series = series.dropna()

# Eliminamos ultima obsercación
series = series.iloc[:-1, :]


st.title(" Análisis de Cointegración y VECM")
st.markdown("Sube un archivo CSV con las series de **Inflación, Tasa de Interés y Tipo de Cambio**")

if series is not None:

    series = series.set_index("fecha")
    # Verificar estacionariedad con prueba ADF
    st.write("## Prueba de Estacionariedad (ADF)")
    
    def adf_test(series):
        result = adfuller(series)
        return {"Estadístico ADF": result[0], "p-value": result[1]}

    # Aplicar prueba ADF a cada serie
    adf_results = {col: adf_test(series[col]) for col in series.columns}
    st.write(pd.DataFrame(adf_results).T)

    # Verificar cointegración con prueba de Johansen
    st.write("## Prueba de Cointegración de Johansen")
    johansen_test = coint_johansen(series, det_order=0, k_ar_diff=1)
    
    # Mostrar valores propios y estadísticos de traza
    eigenvalues = johansen_test.eig
    trace_stats = johansen_test.lr1
    critical_values = johansen_test.cvt[:, 1]  # Usamos el nivel de 5%

    johansen_results = pd.DataFrame({
        "Rango": list(range(len(trace_stats))),
        "Estadístico de Traza": trace_stats,
        "Valor Crítico (5%)": critical_values,
        "¿Cointegración?": ["Sí" if trace_stats[i] > critical_values[i] else "No" for i in range(len(trace_stats))]
    })
    st.write(johansen_results)

    # Determinar el rango de cointegración
    rank = sum(johansen_results["¿Cointegración?"] == "Sí")

    st.write(f"### Rango óptimo de cointegración detectado: {rank}")

    # Ajustar modelo VECM
    st.write("## Estimación del Modelo VECM")

    if rank > 0:
        vecm_model = VECM(series, k_ar_diff=2, coint_rank=rank, deterministic="ci")  # Ajuste según prueba Johansen
        vecm_result = vecm_model.fit()

        # Mostrar matrices clave
        st.write("### Matriz de Cointegración (β)")
        st.write(pd.DataFrame(vecm_result.beta, index=series.columns))

        st.write("### Matriz de Ajuste (α)")
        st.write(pd.DataFrame(vecm_result.alpha, index=series.columns))

        st.write("### Matriz de Corto Plazo (Γ)")
        st.write(pd.DataFrame(vecm_result.gamma, index=series.columns))

        # Gráfico de Impulso-Respuesta
        st.write("## Análisis de Impulso-Respuesta")

        irf = vecm_result.irf(12)  # 12 meses
        fig, ax = plt.subplots(figsize=(10, 6))
        irf.plot(orth=True)
        st.pyplot(fig)

    else:
        st.warning("No se encontró cointegración. Se recomienda usar un modelo VAR en diferencias.")