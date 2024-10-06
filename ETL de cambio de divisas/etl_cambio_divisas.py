############################################################
########                 EXTRAER                 ###########
############################################################

import requests
import os
import json
from dotenv import load_dotenv


def obtener_cambio_fiat(base_currency="EUR"):
    # Cargar las claves API desde .env
    load_dotenv()
    API_KEY = os.getenv("EXCHANGE_RATE_API_KEY")

    # Obter las tasas de cambio
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{base_currency}"
    response = requests.get(url)
    if response.status_code == 200:
        dato = response.json()
        return dato['conversion_rates']  
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    

def obtener_cambio_criptomonedas(ids = 'bitcoin,ethereum,litecoin',
                                moneda_base='eur'):
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies={moneda_base}"
    response = requests.get(url)
    
    if response.status_code == 200:
        dato = response.json()
        
        return dato
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    

############################################################
########              TRANSFORMAR               ###########
############################################################

import pandas as pd

def transformar_datos_cambio_fiat(cambios):
    # Convertir el diccionario de tasas de cambio a un DataFrame
    df = pd.DataFrame(list(cambios.items()), columns=["currency", "exchange_rate"])
    df["Date"] = pd.Timestamp.now()  # Añadir una columna con la fecha actual

    return df


def transformar_datos_cambio_criptomonedas(cambios):
    # Convertir el diccionario de tasas de cambio a un DataFrame
    currencies = ['BTC', 'ETH', 'LTC']
    exchange_rates = [
        cambios['bitcoin']['eur'], 
        cambios['ethereum']['eur'], 
        cambios['litecoin']['eur']
    ]
    
    df = pd.DataFrame({
        'currency': currencies,
        'exchange_rate': exchange_rates
    })
    df["Date"] = pd.Timestamp.now()  # Añadir una columna con la fecha actual

    return df


############################################################
########                   CARGAR                ###########
############################################################

import mysql.connector
from sqlalchemy import create_engine

def cargar_en_mysql(df):
    # Conexión a MySQL
    engine = create_engine(f'mysql+mysqlconnector://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@{os.getenv("MYSQL_HOST")}/{os.getenv("MYSQL_DB")}')
    
    # Cargar los datos en la tabla `exchange_rates`
    df.to_sql('exchange_rates', con=engine, if_exists='append', index=False)
    print("Datos cargados correctamente a la base de datos.")



############################################################
########               EJECUTAR                  ###########
############################################################

try:
    cambios_fiat = obtener_cambio_fiat(moneda_base="EUR")
    cambios_criptomonedas = obtener_cambio_criptomonedas(ids = 'bitcoin,ethereum,litecoin', moneda_base='eur')
    
    datos_transformados_fiat = transformar_datos_cambio_fiat(cambios_fiat)
    datos_transformados_criptomonedas = transformar_datos_cambio_criptomonedas(cambios_criptomonedas)
    datos_transformados = pd.concat([datos_transformados_fiat, datos_transformados_criptomonedas], ignore_index=True)
    
    cargar_en_mysql(datos_transformados)

except Exception as e:
    print(f"Error durante el proceso ETL: {e}")