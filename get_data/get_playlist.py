#######################################################################################################################
# Obtener datos de lista de reproducción de Youtube para crear el conjunto de datos de CB                             #   
# Autor: jairomelo                                                                                                    #
# Fecha: 2023-03-09                                                                                                   #
# Versión: 1.0                                                                                                        #
# Python Version: 3.10.4                                                                                              #
#######################################################################################################################

import pandas as pd
from pytube import Playlist, YouTube
from pytube.exceptions import PytubeError
import time
import re

METADATA =  ["objectid","filename","youtubeid","title","creator","date","description","subject","location","latitude","longitude","source","identifier","type","format","language","rights","rightsstatement"]

def prepare_df(df):
    """ Preparar el dataframe para crear el CSV
    PARAMETROS
    ----------
    df: pandas.DataFrame
        Dataframe con los metadatos de los videos
    RETORNO
    -------
    df: pandas.DataFrame
        Dataframe con los metadatos de los videos
    """

    df = df.drop_duplicates(subset="identifier", keep="first")
    df = df.sort_values(by="identifier", ascending=False)
    df = df.reset_index(drop=True)

    return df

def crear_datos(url_list: str, origen_datos: str, patron: str) -> pd.DataFrame:
    """ Obtener metadatos de videos de Youtube y crear o actualizar un dataframe con ellos
    PARAMETROS
    ----------
    url_list: str
        URL de la lista de reproducción de Youtube
    origen_datos: str
        Ruta del archivo CSV con los metadatos de los videos
    patron: str
        Patrón para eliminar palabras clave que no son relevantes
    RETORNO
    -------
    df: pandas.DataFrame
        Dataframe con los metadatos de los videos
    """

    p = Playlist(url_list)

    try:
        df = pd.read_csv(origen_datos)
    except FileNotFoundError:
        df = pd.DataFrame(columns=METADATA)


    for i, video in enumerate(p.video_urls):
        if video.replace("www.", "") not in df["identifier"].tolist(): # Eliminar www. para evitar duplicados
            yt = YouTube(video)

            # Obtener metadatos

            try:
                title = yt.title
                print(i, title, video)
            except PytubeError:
                print(i, video)
                break

        
            keywords = yt.keywords
            if len(keywords) > 1:
                for i, keyword in enumerate(keywords):
                    eliminar = re.findall(patron, keyword)
                    elemento_limpio = re.sub(patron, "", keyword)
                    keywords[i] = elemento_limpio
                keywords = "; ".join(keywords)
                keywords = keywords.replace("  ;", "")
            elif len(keywords) == 1:
                keywords = keywords[0]
                if re.findall(patron, keywords):
                    keywords = ""
            else:
                keywords = ""

            try:
                mimetype = yt.vid_info['streamingData']['formats'][0]['mimeType'].split(";")[0]
            except KeyError:
                mimetype = "video/3gpp"
            except IndexError:
                mimetype = "video/3gpp"

            # Crear dataframe

            elemento = pd.DataFrame({
                "objectid": [yt.video_id],
                "filename": [""],
                "youtubeid": [yt.video_id],
                "title": [title],
                "creator": [yt.author],
                "date": [yt.publish_date],
                "description": [yt.description],
                "subject": [keywords],
                "location": [""],
                "latitude": [""],
                "longitude": [""],
                "source": [yt.channel_url],
                "identifier": [yt.watch_url],
                "type": ["Image;MovingImage"],
                "format": [mimetype],
                "language": [""],
                "rights": ["Licencia de YouTube estándar"],
                "rightsstatement": ["https://www.youtube.com/static?template=terms"]
            })

            df = pd.concat([df, elemento], ignore_index=True)

            time.sleep(2)

        else:
            print(f"El video {video} ya está en el conjunto de datos", end="\r", flush=True)

    df = prepare_df(df)

    return df

if __name__ == "__main__":
    patron = r"\b\w{32}\b|\b\d{5}\b"
    url_list = "https://www.youtube.com/playlist?list=PL2JptZL3cKEP0M9Tn-ts_5I1XKTYxL9yU"
    origen_datos = "_data/data.csv"
    df = crear_datos(url_list, origen_datos, patron)
    df.to_csv(origen_datos, index=False)
