#############################
# Obtener datos de lista de reproducción de Youtube para crear el conjunto de datos de CB
# Autor: jairomelo
# Fecha: 2023-03-09
# Versión: 1.0
# Python Version: 3.10.4
#############################

import pandas as pd
from pytube import Playlist, YouTube
from pytube.exceptions import PytubeError
import time
import re

METADATA =  ["objectid","filename","youtubeid","title","creator","date","description","subject","location","latitude","longitude","source","identifier","type","format","language","rights","rightsstatement"]

patron = r"\b\w{32}\b|\b\d{5}\b"

url_list = "https://www.youtube.com/playlist?list=PL2JptZL3cKEP0M9Tn-ts_5I1XKTYxL9yU"

p = Playlist(url_list)

try:
    df = pd.read_csv("_data/data.csv")
except FileNotFoundError:
    df = pd.DataFrame(columns=METADATA)

for i, video in enumerate(p.video_urls):
    yt = YouTube(video)
    try:
        title = yt.title
        print(i, title, video)
    except PytubeError:
        print(i, video)
        break

    if title not in df["title"].values:
        keywords = yt.keywords
        if len(keywords) > 1:
            for i, keyword in enumerate(keywords):
                eliminar = re.findall(patron, keyword)
                elemento_limpio = re.sub(patron, "", keyword)
                keywords[i] = elemento_limpio
            keywords = "; ".join(keywords)
        elif len(keywords) == 1:
            keywords = keywords[0]
            if re.findall(patron, keywords):
                keywords = ""
        else:
            keywords = ""

        mimetype = yt.vid_info['streamingData']['formats'][0]['mimeType'].split(";")[0]

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


df.to_csv("_data/data.csv", index=False)