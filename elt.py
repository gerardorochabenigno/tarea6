import boto3
import awswrangler as wr
import logging
import os
import yaml

# Variables del config
with open("config/config.yaml", "r") as file:
    config = yaml.safe_load(file)

LOG_PATH = config['log_elt'] 


# Configuración del log
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")



if __name__ == "__main__":
    try:

        session = boto3.Session(profile_name='datascientist')
        s3 = session.client('s3')


        query = """
        DROP TABLE IF EXISTS econ.inflacion;
        """

        wr.athena.read_sql_query(
            query, 
            database="econ", 
            ctas_approach=False, 
            boto3_session=session
        )

        query = """

        CREATE EXTERNAL TABLE econ.inflacion (
            fecha DATE,
            inflacion DOUBLE
        ) COMMENT 'Inflacion mensual'
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES ('field.delim' = ',')
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://itam-analytics-grb/econ/raw/inflacion/'
        TBLPROPERTIES ("skip.header.line.count"="1");

        """

        wr.athena.read_sql_query(
            query, 
            database="econ", 
            ctas_approach=False, 
            boto3_session=session
        )

        logging.info("Tabla inflación creada con éxito.")

        query = """
                DROP TABLE IF EXISTS econ.tasa_de_interes;
                """

        wr.athena.read_sql_query(
            query, 
            database="econ", 
            ctas_approach=False, 
            boto3_session=session
        )

        query = """

                CREATE EXTERNAL TABLE econ.tasa_de_interes (
                    fecha DATE,
                    tasa_de_interes DOUBLE
                ) COMMENT 'Tasa e interés diaria'
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                WITH SERDEPROPERTIES ('field.delim' = ',')
                STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION 's3://itam-analytics-grb/econ/raw/tasa_de_interes/'
                TBLPROPERTIES ("skip.header.line.count"="1");

                """

        wr.athena.read_sql_query(
            query, 
            database="econ", 
            ctas_approach=False, 
            boto3_session=session
        )

        logging.info("Tabla tasa de interés creada con éxito.")

        query = """
        DROP TABLE IF EXISTS econ.tipo_de_cambio;
        """

        wr.athena.read_sql_query(
            query, 
            database="econ", 
            ctas_approach=False, 
            boto3_session=session
        )

        query = """

        CREATE EXTERNAL TABLE econ.tipo_de_cambio (
            fecha DATE,
            tipo_de_cambio DOUBLE
        ) COMMENT 'Tipo de cambio diario diaria'
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES ('field.delim' = ',')
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://itam-analytics-grb/econ/raw/tipo_de_cambio/'
        TBLPROPERTIES ("skip.header.line.count"="1");

        """

        wr.athena.read_sql_query(
            query, 
            database="econ", 
            ctas_approach=False, 
            boto3_session=session
        )
        logging.info("Tabla tipo de cambio creada con éxito.")

    except Exception as e:
        logging.error(f"Error en el ELT: {e}")
        print(f"Error en el ELT. Revisa el log.")

