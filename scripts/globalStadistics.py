#!/usr/bin/python

from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import string
import pandas as pd
import numpy as np
import json
from pyspark.sql.functions import col, create_map, lit
from itertools import chain
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import matplotlib.patches as mpatches
import seaborn as sb
import sys
"""

	Este srcript nos permite sacar estadisticas a nivel global.
	Segun la opcion que seleccionemos podemos sacar
	estadisticas de la categoria, meses y anyo:
		- month_statistics
		- global_category
		- year_statistics
	


"""
# diccionario con las abreviaturas y el nombre completo del pais

countries = {'CA':'Canada',
		'DE':'Alemania',
		'FR':'Francia',
		'GB':'Reino Unido',
		'IN':'India',
		'JP':'Japon',
		'KR':'Korea',
		'MX':'Mexico',
		'RU':'Rusia',
		'US':'Estados Unidos'}


def start(topic):

    conf = SparkConf().setMaster('local').setAppName('TOP_Category')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Estructura para definir las columnas y tipos de nuestro sqlContext

    struct1 = StructType([StructField("video_id", StringType(), True),
                          StructField("trending_date", StringType(), True),
                          StructField("title", StringType(), True),
                          StructField("channel_title", StringType(), True),
                          StructField("category_id", IntegerType(), True),
                          StructField("publish_time", StringType(), True),
                          StructField("tags", StringType(), True),
                          StructField("views)", StringType(), True),
                          StructField("likes", StringType(), True),
                          StructField("dislikes", StringType(), True),
                          StructField("comment_count", StringType(), True),
                          StructField("thumbnail_link", StringType(), True),
                          StructField("comments_disabled", StringType(), True),
                          StructField("ratings_disabled", StringType(), True),
                          StructField("video_error_or_removed",
                                      StringType(), True),
                          StructField("description", StringType(), True)])

    prefijos = countries.keys()

    total_dataframe = pd.DataFrame()

    # En total_dataframe juntaremos los datos de todos los paises
    # para poder sacar estadisticas globales

    for prefix in prefijos:
        ruta = '../data/'+prefix+'videos.csv'
        df = pd.read_csv(ruta)
        total_dataframe = pd.concat([total_dataframe, df])

    if (topic == "YEAR"):
        year_statistics(total_dataframe)
    elif (topic == "MONTH"):
        month_statistics(total_dataframe)
    else:
        global_category(total_dataframe)


def month_statistics(total_dataframe):

    total_dataframe['publish_time'] = total_dataframe['publish_time'].apply(
        lambda x: str(x)[5:7])

    resultados = total_dataframe.groupby('publish_time').mean()
    #resultados = total_dataframe.groupBy("publish_time").mean()
    print(resultados)
    resultados['views'] = resultados['views'].astype(int)
    fig = plt.figure(figsize=(12, 6))

    axes = fig.add_subplot(111)

    axes.plot(resultados.views, marker='o')

    axes.set(ylabel='Numero de visitas totales', xlabel='Mes',
             title='Meses con mas visitas de media')
    plt.savefig('month_statistics.png')

    pass


def global_category(total_dataframe):

    category_list = get_categories()
    df = total_dataframe.replace({"category_id": category_list})
    f = sb.catplot('category_id', data=df, kind="count", aspect=3,
                   hue_order=category_list.values(), order=category_list.values())
    f.set_xticklabels(fontsize=11)
    plt.xlabel("Categoria")
    plt.ylabel("Numero de videos en tendencias")
    plt.title("Numero de videos en tendencias globales")
    fig = plt.gcf()
    fig.set_size_inches(30, 10.5)
    fig.savefig('categoriaTopGlobal.png', dpi=100)

    return df


def year_statistics(total_dataframe):

    total_dataframe['publish_time'] = total_dataframe['publish_time'].apply(
        lambda x: str(x)[:4])

    resultados = total_dataframe.groupby('publish_time').mean()
    #resultados = total_dataframe.groupBy("publish_time").mean()
    print(resultados)
    resultados['views'] = resultados['views'].astype(int)
    fig = plt.figure(figsize=(12, 6))

    axes = fig.add_subplot(111)

    axes.plot(resultados.views, marker='o')

    axes.set(ylabel='Numero de visitas totales', xlabel='Mes',
             title='Meses con mas visitas de media')
    plt.savefig('year_statistics.png')

    pass


def get_categories():

    # Nos permite obtener el nombre de las categorias
    # y su id, para poder remplazarlas en los datos

    with open('../data/CA_category_id.json') as json_file:
        data = json.load(json_file)
        store_list = dict()
        for item in data['items']:
            index = int(item['id'])
            store_list[index] = item['snippet']['title']
        print(store_list.values())

    return store_list


if __name__ == "__main__":
    # ARGUMENT PARSER
    import argparse
    parser = argparse.ArgumentParser()
    helpRegionCode = 'Region code for the youtube videos, by default ALL.\nPossible regions:\nCA: Canada,\n\tDE: Alemania,\n\tFR: Francia,\n\tGB: Reino Unido,\n\tIN: India,\n\tJP: Japon,\n\tKR: Korea,\n\tMX: Mexico,\n\tRU: Rusia,\n\tUS: Estados Unidos'
    parser.add_argument(
        "topic", help="Available options: year, month, category", default="ALL")
    args = parser.parse_args()
    # END OF ARGUMENT PARSER

    topic = args.topic.upper()

    if (topic not in ["YEAR", "MONTH", "CATEGORY"]):
        sys.exit(1)
        pass

    start(topic)
