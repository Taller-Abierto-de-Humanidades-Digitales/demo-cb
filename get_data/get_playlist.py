#######################################################################################################################
# Obtener datos de lista de reproducción de Youtube para crear el conjunto de datos de CB                             #   
# Autor: jairomelo                                                                                                    #
# Fecha: 2023-03-09                                                                                                   #
# Versión: 1.0                                                                                                        #
# Python Version: 3.10.4                                                                                              #
#######################################################################################################################

import math
import os
import pandas as pd
from pytube import Playlist, YouTube
from pytube.exceptions import PytubeError
import time
import re
from PIL import Image
import requests
from io import BytesIO
import cv2
import numpy as np
from datetime import date

METADATA =  ["objectid","filename","youtubeid","title","creator","date","creationdate","description","subject","location","latitude","longitude","source","identifier","type","format","language","rights","rightsstatement","addingdate"]
    
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
            print("Intentando guardar metadatos de video y miniatura")
            yt = YouTube(video)

            if not os.path.exists(f"thumbnails/{yt.video_id}.jpg"):
                # guardar thumbnail
                try:
                    os.makedirs("thumbnails", exist_ok=True)
                    thumbnail = yt.thumbnail_url
                    thumbnail_r = requests.get(thumbnail, stream=True)
                    if thumbnail_r.status_code != 200 and int(thumbnail_r.headers['Content-length']) <= 1097:
                        print(f"Error al guardar la miniatura del video {video}")
                        pass
                    else:
                        thumbnail = Image.open(BytesIO(thumbnail_r.content))
                        thumbnail.save(f"thumbnails/{yt.video_id}.jpg")
                        print(f"Miniatura del video {video} guardada correctamente.")
                        print(thumbnail_r.headers['Content-length'])
                        time.sleep(3)
                    
                except Exception as e:
                    print(e)
                    print(f"Error al guardar la miniatura del video {video}")
                    pass

            # guardar video
            """ try:
                os.makedirs("videos_backup", exist_ok=True)
                yt.streams.filter(progressive=True, file_extension="mp4").order_by("resolution").desc().first().download("videos_backup")
            except Exception as e:
                print(e)
                print(f"Error al guardar el video {video}")
                pass
            """

            # Obtener metadatos

            try:
                title = yt.title
                print(i, title, video)


        
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
                    "creationdate": [""],
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
                    "rightsstatement": ["https://www.youtube.com/static?template=terms"],
                    "addingdate": [date.today()]
                })

                df = pd.concat([df, elemento], ignore_index=True)

                time.sleep(2)

            except PytubeError as e:
                print(f"PytubeError: {i}, {video}. {e}")
                pass

        elif video.replace("www.", "") in df["identifier"].tolist() and not os.path.exists(f"thumbnails/{video.split('=')[-1]}.jpg"):
            print("Intentando guardar miniatura")
            yt = YouTube(video)
            # guardar thumbnail
            try:
                os.makedirs("thumbnails", exist_ok=True)
                thumbnail = yt.thumbnail_url
                thumbnail_r = requests.get(thumbnail, stream=True)
                if thumbnail_r.status_code != 200 and int(thumbnail_r.headers['Content-length']) <= 1097:
                    print(f"Error al guardar la miniatura del video {video}")
                    pass
                else:
                    thumbnail = Image.open(BytesIO(thumbnail_r.content))
                    thumbnail.save(f"thumbnails/{yt.video_id}.jpg")
                    print(f"Miniatura del video {video} guardada correctamente.")
                    print(thumbnail_r.headers['Content-length'])
                    time.sleep(3)
                
            except Exception as e:
                print(e)
                print(f"Error al guardar la miniatura del video {video}")
                pass

        else:
            print(f"El video {video} ya está en el conjunto de datos")

    df = prepare_df(df)

    reporte = f"""
    
    Reporte de la lista de reproducción
    ---------------------
    Número de videos: {len(p.video_urls)}
    Número de videos en el conjunto de datos: {len(df)}
    Cantidad de thumbnails: {len(os.listdir("thumbnails"))}

    Por procesar: 
    - Videos: {len(p.video_urls) - len(df)}
    - Thumbnails: {len(p.video_urls) - len(os.listdir("thumbnails"))}
    """

    print(reporte)

    return df

def create_banner_img(max_width, max_height):
    imagenes = os.listdir("thumbnails")
    imagenes = [os.path.join("thumbnails", i) for i in imagenes if i.endswith(".jpg")]
    n_imagenes = len(imagenes)
    n_columnas = calculate_n_columns(imagenes, max_width, max_height)
    #print(n_columnas)
    n_filas = (n_imagenes + n_columnas - 1) // n_columnas  # Redondear hacia arriba la división entera

    ancho_imagenes = round(max_width / n_columnas)
    alto_imagenes = round(max_height / n_filas)

    max_dim = max(max_width, max_height)
    imagenes = [cv2.imread(imagen) for imagen in imagenes]
    imagenes = [resize_and_crop_image(i, max_dim, max_dim) for i in imagenes]
    imagenes = [cv2.resize(i, (ancho_imagenes, alto_imagenes)) for i in imagenes]

    banner = np.zeros((max_height, max_width, 3), dtype=np.uint8)

    for i, imagen in enumerate(imagenes):
        try:
            fila = i // n_columnas
            columna = i % n_columnas
            banner[fila*alto_imagenes:(fila+1)*alto_imagenes, columna*ancho_imagenes:(columna+1)*ancho_imagenes, :] = imagen
        except ValueError:
            pass

    cv2.imwrite("objects/portada.png", banner)

def calculate_n_columns(imagenes, max_width, max_height):
    total_area = sum(cv2.imread(i).shape[0] * cv2.imread(i).shape[1] for i in imagenes)
    aspect_ratio = float(max_width) / max_height
    n_columns = math.ceil(math.sqrt(total_area / aspect_ratio) / max_height)
    return n_columns

def resize_and_crop_image(image, max_width, max_height):
    height, width = image.shape[:2]
    if height > width:
        ratio = float(max_height) / height
        new_height = max_height
        new_width = int(width * ratio)
    else:
        ratio = float(max_width) / width
        new_width = max_width
        new_height = int(height * ratio)

    resized_image = cv2.resize(image, (new_width, new_height), interpolation=cv2.INTER_AREA)

    if new_width > max_width:
        x_start = (new_width - max_width) // 2
        x_end = x_start + max_width
        cropped_image = resized_image[:, x_start:x_end]
    elif new_height > max_height:
        y_start = (new_height - max_height) // 2
        y_end = y_start + max_height
        cropped_image = resized_image[y_start:y_end, :]
    else:
        cropped_image = resized_image

    return cropped_image


if __name__ == "__main__":
    patron = r"\b\w{32}\b|\b\d{5}\b"
    url_list = "https://www.youtube.com/playlist?list=PL2JptZL3cKEP0M9Tn-ts_5I1XKTYxL9yU"
    origen_datos = "_data/data.csv"
    df = crear_datos(url_list, origen_datos, patron)
    df.to_csv(origen_datos, index=False)
    create_banner_img(1050, 660)
