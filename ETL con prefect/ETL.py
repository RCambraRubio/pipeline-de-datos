import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta 
import pyodbc
from credentials import credentials

from prefect import task, Flow
from prefect.schedules import IntervalSchedule

@task(log_stdout=True)
def extract(urls):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    
    datos = []
    
    today = datetime.today().strftime('%Y-%m-%d')
    
    # Para cada url 
    for url_key in urls.keys():
        url = urls[url_key]
        # Obtener la pagina
        html = requests.get(url, headers= headers)
        # Importar en BS
        soup = BeautifulSoup(html.content, 'html.parser')
        
        # Guardar los datos
        try:
            valoraciones = soup.find_all("span", {"class": "EymY4b"})[0].text.strip().split(" ")[0]
            puntuacion = soup.find_all("div", {"class": "BHMmbe"})[0].text.strip()
            descargas = soup.find_all("span",{"class":"htlgb"})[4].text.replace('+','').strip()
            datos.append({'fecha': today, 'Nombre' : url_key , 'valoraciones' : valoraciones, 'puntuacion' : puntuacion, 'descargas' : descargas})
        except:
            print("Error: Ha habido un problema al cargar los datos de {}".format(url_key))

    return datos


@task(log_stdout=True)
def  transform(raw):
    datos_limpios = raw.copy()

    for dato in raw:
        # Las valoraciones deben ser enteros y mayores que 0
        try:
            dato['valoraciones'] =int(dato['valoraciones'].replace(',',''))
            if(dato['valoraciones']<0):
                print("El dato extraido en el campo de valoraciones es menor que 0 por lo que se ha eliminado el registro perteneciente a {}".format(dato['Nombre']))
                datos_limpios.remove(dato)
        except:
            print("Error: El dato extraido en el campo de valoraciones no es entero por lo que se ha elimidado el registro perteneciente a {}".format(dato['Nombre']))
            datos_limpios.remove(dato)
        # Las puntuaciones deben ser ser decimales y estar comprendidas entre 0 y 5
        try:
            dato['puntuacion'] =float(dato['puntuacion'])
            if(dato['puntuacion']>5.0 or dato['puntuacion']<0.0):
                print("El dato extraido en el campo de puntuacion es mayor que 5 o menor que 0 por lo que se ha eliminado el registro perteneciente a {}".format(dato['Nombre']))
                datos_limpios.remove(dato)    
        except:
            print("Error: El dato extraido en el campo de puntuacion no es decimal por lo que se ha elimidado el registro perteneciente a {}".format(dato['Nombre']))
            datos_limpios.remove(dato)
        # Las descargas deben ser enteros y mayores que 0
        try:
            dato['descargas'] =int(dato['descargas'].replace(',',''))
            if(dato['descargas']<0):
                print("El dato extraido en el campo de descargas es menor que 0 por lo que se ha eliminado el registro perteneciente a {}".format(dato['Nombre']))
                datos_limpios.remove(dato)
        except:
            print("Error: El dato extraido en el campo de descargas no es entero por lo que se ha elimidado el registro perteneciente a {}".format(dato['Nombre']))
            datos_limpios.remove(dato)
    
    # Convertir los datos limpios a un dataframe
    df = pd.DataFrame(datos_limpios)
    
    return df
    
    
@task(log_stdout=True)
def load(transformed):
    server = credentials.server
    database = credentials.database
    username = 'admin_ud'
    password = credentials.password
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
    cursor = cnxn.cursor()

    # Insertar valores
    for index,row in transformed.iterrows():
        cursor.execute('INSERT INTO dbo.playstore VALUES (?,?,?,?,?)', row.tolist())
        cnxn.commit()
    cursor.close()
    cnxn.close()



schedule = IntervalSchedule(interval = timedelta(days=1))


with Flow("play store", schedule) as flow:
    # urls
    urls = {
        "parchis_star": "https://play.google.com/store/apps/details?id=com.superking.parchisi.star&hl=es_419&gl=US",
        "pokemon_go": "https://play.google.com/store/apps/details?id=com.nianticlabs.pokemongo&hl=es_419&gl=US",
        "mario_kart_tour": "https://play.google.com/store/apps/details?id=com.nintendo.zaka&hl=es_419&gl=US",
        "whatsapp": "https://play.google.com/store/apps/details?id=com.whatsapp&hl=es_419&gl=US",
        "spotify": "https://play.google.com/store/apps/details?id=com.spotify.music&hl=es_419&gl=US"
    }
    
    raw = extract(urls)
    transformed = transform(raw)
    load(transformed)
    
flow.run()