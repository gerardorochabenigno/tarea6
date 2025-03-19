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
import plotly.express as px

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
series = series.dropna() # Queremos una serie sin valores faltantes
series = series.loc[series["fecha"] >= "2000-01-01"]  # Filtrar desde el 2000

# Configuración de la página
st.set_page_config(page_title="Relación entre tipo de cambio, tasa de interés e inflación", layout="wide")

# Encabezado proncipal
st.title("Relación entre tipo de cambio, tasa de interés e inflación en México")
st.write("Esta aplicación muestra la relación entre el tipo de cambio, la tasa de interés y la inflación en México. La información proviene de Banco de México e INEGI y contiene información mensual del tipo de cambio, tasa de interés e inflación desde enero del 2000 hasta la actualidad.")
st.write("En teoría monetaria, se espera que exista una relación de largo plazo entre la inflación y la tasa de interés, debido a que el objetivo prioritario de los bancos centrales es mantener la inflación baja y estable. Por otro lado, el tipo de cambio es un indicador clave para la economía de un país, ya que afecta la competitividad de las exportaciones y el poder adquisitivo de los consumidores.")
st.write("En esta aplicación, se realiza un análisis exploratorio de la información y, posteriormente, se estima un modelo de corrección de error (VECM) para descomponer las series en sus componentes de corto y largo plazo.")

# Análisis exploratorio
st.title("Análisis Exploratorio")
if series is not None:
    # Mostrar las primeras filas de los datos
    st.write("### Encabezado de la serie")
    st.write(series.tail())

    # Agregamos una grafica de las series con barra para seleccionar el rango
    st.write("### Gráficós de interés")
    st.write("#### Gráfico de la evolución")
    # Convertir a formato largo (long format) para usar Plotly Express
    series_long = series.melt(id_vars=["fecha"], 
                          var_name="variable", 
                          value_name="valor")
    # Crear gráfico con Plotly
    fig = px.line(series_long, 
              x="fecha", 
              y="valor", 
              color="variable", 
              #title="EVOLUCIÓN DE LA TASA DE INTERÉS, TIPO DE CAMBIO E INFLACIÓN",
              labels={"fecha": "Fecha", "valor": "Valor", "variable": "Indicador"})
    
    fig.update_layout(
        height=600,  # Ajustar la altura
        width=800,   # Ajustar la anchura (puedes reducirlo si quieres más delgado)
        xaxis=dict(
            rangeslider=dict(visible=True), 
            type="date"
        ),
        yaxis=dict(fixedrange=False),  # Permitir zoom en Y
        legend=dict(
            title="Indicador", 
            orientation="h",  # Mostrar leyenda horizontalmente
            yanchor="bottom", y=  1.02,  # Subir leyenda arriba de la gráfica
            xanchor="right", x=1
        )
    )
    # Mostrar en Streamlit
    st.plotly_chart(fig, use_container_width=True)

    # Gráfica de dispersón entre tipo de cambio y tasa de interés
    st.write("#### Gráfica de dispersión entre tipo de cambio y tasa de interés")
    st.scatter_chart(data=series, x="tipo_de_cambio_mensual", y="tasa_de_interes_mensual", use_container_width=True)

    # Gráfica de dispersión entre tasa de interés e inflación
    st.write("#### Gráfica de dispersión entre tasa de interés e inflación")
    st.scatter_chart(data=series, x="tasa_de_interes_mensual", y="inflacion_mensual", use_container_width=True)

    # Gráfica de dispersión entre tipo de cambio y inflación
    st.write("#### Gráfica de dispersión entre tipo de cambio y inflación")
    st.scatter_chart(data=series, x="tipo_de_cambio_mensual", y="inflacion_mensual", use_container_width=True)

    


st.title("Análisis de Cointegración y Modelo de Corrección de Error (VECM)")

texto = """
En Econometría y Estadística, cointegración describe una relación de largo plazo y estable entre dos o más series de tiempo no estacionarias donde su combinación lineal es estacionaria, incluso si no todas las series individuales son estacionarias.
Para saber si existe cointegración entre las series de tiempo, es necesario realizar pruebas estadísticas. La primera prueba a realizar es de estacionariedad, usando la prueba de Dickey-Fuller Aumentada (ADF), si todas las series son estacionarias, podemos usar un modelo VAR.
Si no totdas las series son son estacionarias, se procede a realizar una prueba de cointegración a través de la prueba de Johansen. Si la prueba de Johanssen arroja que existe cointegración entre las series, 
se procede a ajustar un modelo de Corrección de Error Vectorial (VECM) para descomponer las series en sus componentes de corto y largo plazo.

"""
st.write(texto)

if series is not None:
    series = series.set_index("fecha")
    # Verificar estacionariedad con prueba ADF
    st.write("## Prueba de Estacionariedad (ADF)")
    texto = """
    La prueba de Dickey-Fuller Aumentada (ADF) es una prueba estadística que prueba la hipotesis núla de una serie de tiempo presenta una raíz unitaria.
    La serie de tiempo es estacionaria si el p-value es menor a 0.05.
    """
    st.write(texto)
    
    def adf_test(series):
        result = adfuller(series)
        return {"Estadístico ADF": result[0], "p-value": result[1]}

    # Aplicar prueba ADF a cada serie
    adf_results = {}
    for col in series.columns:
        test_result = adfuller(series[col])
        adf_results[col] = {
            "Estadístico ADF": test_result[0],
            "p-value": test_result[1],
            "Valores Críticos": test_result[4],
            "¿Estacionaria?": "Sí" if test_result[1] < 0.05 else "No"
        }

    # Convertir a DataFrame y transponer para mejor visualización
    adf_results_df = pd.DataFrame(adf_results).T
    st.write(adf_results_df.drop(columns="Valores Críticos"))

    # Verificar cointegración con prueba de Johansen
    st.write("## Prueba de Cointegración de Johansen")
    texto = """

La prueba de Johansen es un método estadístico que permite determinar si existe **cointegración** entre dos o más series de tiempo. Se basa en un modelo **VAR (Vector AutoRegresivo)** y analiza la existencia de **relaciones de equilibrio a largo plazo** entre las variables.

- Para determinar si las series están cointegradas, la prueba de Johansen utiliza **valores propios (eigenvalues)** de la matriz de cointegración y los compara con valores críticos en dos pruebas:

  - **Trace Test:** Evalúa cuántas relaciones de cointegración existen.
  - **Maximum Eigenvalue Test:** Examina si hay al menos una relación de cointegración en los datos.

- Si los valores estadísticos de estas pruebas son **mayores** que los valores críticos:
  - **Se rechaza la hipótesis nula de "no cointegración"**.
  - **Se concluye que las series están cointegradas**.

    """
    st.markdown(texto)
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

    st.write(f"#### Rango óptimo de cointegración detectado: {rank}")

    # Ajustar modelo VECM
    st.write("## Estimación del Modelo VECM")

    texto = r"""
El **Modelo de Corrección de Error (VECM)** es una extensión del **VAR (Vector AutoRegresivo)** que permite modelar tanto:
- La **relación de largo plazo** entre variables cointegradas.
- Los **ajustes de corto plazo** cuando las variables se desvían del equilibrio.

El VECM se define como:

$$
\Delta Y_t = \Gamma_1 \Delta Y_{t-1} + \Gamma_2 \Delta Y_{t-2} + \dots + \Gamma_{k-1} \Delta Y_{t-k+1} + \Pi Y_{t-1} + \varepsilon_t
$$

Donde:
- \( $Y_t$ \) es un vector de variables **no estacionarias pero cointegradas**.
- \( $\Delta Y_t$ \) representa los cambios en \( $Y_t$ \), capturando las relaciones de **corto plazo**.
- \( $\Gamma_i$ \) son los coeficientes que miden la influencia de los rezagos en las diferencias.
- \( $\Pi Y_{t-1}$ \) representa el **término de corrección de error (ECT, Error Correction Term)**, que ajusta las desviaciones del equilibrio de largo plazo.
- \( $\varepsilon_t$ \) es el término de error.

Donde:
El **término de corrección de error** se expresa como:

$$
\Pi = \alpha \beta'
$$

Donde:
- \( $\beta$ \) representa los **vectores de cointegración**, que describen la relación de equilibrio entre las variables.
- \( $\alpha$ \) representa la **velocidad de ajuste** hacia el equilibrio.

Interpretación de \( $\alpha$ \):
- **Si \( $\alpha$ \) es negativo y significativo**, significa que la variable **regresa al equilibrio tras una desviación**.
- **Si \( $\alpha$ \) es cercano a cero**, la **corrección es lenta**.
- **Si \( $\alpha$ \) es positivo**, puede haber **inestabilidad en el sistema**.
"""
    # Mostrar resumen del modelo
    st.write(texto)

    if rank > 0:
        vecm_model = VECM(series, k_ar_diff=1, coint_rank=rank, deterministic="ci")  # Ajuste según prueba Johansen
        vecm_result = vecm_model.fit()

        # Mostrar matrices clave
        st.write("### Matriz de Cointegración (β)")
        st.write(pd.DataFrame(vecm_result.beta, index=series.columns))

        st.write("### Matriz de Ajuste (α)")
        st.write(pd.DataFrame(vecm_result.alpha, index=series.columns))

        st.write("### Matriz de Corto Plazo (Γ)")
        st.write(pd.DataFrame(vecm_result.gamma, index=series.columns))

#        # Gráfico de Impulso-Respuesta
#        st.write("### Gráficas de Impulso-Respuesta")
#
#        texto = """
#        El gráfico de Impulso-Respuesta (IRF) muestra cómo las variables responden a un **impulso** en el tiempo. Se utiliza para analizar la **respuesta de corto plazo** de las variables a un **impulso** en el tiempo.
#        """
#
#        st.write(texto)
#        # Obtener la función de respuesta al impulso
#        irf = vecm_result.irf(10)  # 10 períodos de simulación
#
#        # Convertir los resultados de IRF a un DataFrame para Plotly
#        impulse_vars = series.columns  # Variables en el modelo
#        irf_list = []
#
#        for impulse in impulse_vars:  # Variable que recibe el impulso
#            for response in impulse_vars:  # Variable que responde al impulso
#                for t in range(11):  # 0 a 10 períodos
#                    irf_list.append({
#                        "Periodo": t,
#                        "Variable Impulsada": impulse,
#                        "Variable que Responde": response,
#                        "Respuesta": irf.irfs[t, series.columns.get_loc(response), series.columns.get_loc(impulse)]
#                    })
#
#        # Convertir a DataFrame
#        irf_df = pd.DataFrame(irf_list)
#
#        # Crear gráfico interactivo con Plotly
#        fig = px.line(
#            irf_df,
#            x="Periodo",
#            y="Respuesta",
#            color="Variable que Responde",
#            facet_col="Variable Impulsada",
#            title="Gráfica de Impulso-Respuesta"
#        )
#
#        # Mostrar gráfico en Streamlit
#        st.plotly_chart(fig, use_container_width=True)

    else:
        st.warning("No se encontró cointegración. Se recomienda usar un modelo VAR en diferencias.")