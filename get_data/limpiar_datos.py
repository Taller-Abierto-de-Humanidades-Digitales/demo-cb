#######################################################################################################################
# Limpiar etiquetas y descripciones de los metadatos obtenidos de los videos de Youtube                               #   
# Autor: jairomelo                                                                                                    #
# Fecha: 2023-03-09                                                                                                   #
# Versión: 1.0                                                                                                        #
# Python Version: 3.10.4                                                                                              #
#######################################################################################################################

import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
import re

nltk.download("stopwords")

stop = set(stopwords.words("spanish"))

stopwords_personal = ['35_a', 'ar-', 'i', 'ii']

for word in stopwords_personal:
    stop.add(word)

def reemplazar(patron: str, texto: str, reemplazo: str) -> str:
    """ Reemplazar palabras clave en los metadatos de los videos
    PARAMETROS
    ----------
    patron: str
        Patrón para eliminar palabras clave que no son relevantes
    texto: str
        Texto a limpiar
    reemplazo: str
        Texto para reemplazar las palabras clave
    RETORNO
    -------
    texto: str
        Texto limpio
    """
    # print(texto)

    if texto is not np.nan:
        
        clean_strings = []
        for string in texto.split("; "):
            if string not in stop:
                clean_string = re.sub(patron, reemplazo, string).replace("; ", ";").replace("; ;", ";").strip().lower()
                clean_strings.append(clean_string)
        
        list_to_string = "; ".join(clean_strings)

        return list_to_string

    else:
        return texto

def limpiar_etiquetas(df: pd.DataFrame, patron: str) -> pd.DataFrame:
    """ Limpiar etiquetas de los metadatos de los videos
    PARAMETROS
    ----------
    df: pandas.DataFrame
        Dataframe con los metadatos de los videos
    patron: str
        Patrón para eliminar palabras clave que no son relevantes
    RETORNO
    -------
    df: pandas.DataFrame
        Dataframe con los metadatos de los videos
    """

    # aplicar la función reemplazar a la columna "subject"
    df["subject"] = df["subject"].apply(lambda x: reemplazar(patron, x, ""))

    return df

if __name__ == "__main__":
    df = pd.read_csv("_data/data.csv")
    pattern = r'\b(;|\d+|a|de|del|i+|pt1|la|se|que|por)\b|\s?;\s?'

    df = limpiar_etiquetas(df, pattern)
    df.to_csv("_data/data.csv", index=False)
