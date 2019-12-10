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
import datetime 

"""

	Este srcript nos permite sacar estadisticas de
  Espana en tiempo real.
		- Dias de la semana con mas visitas
    - Canales con mayor proporcion de views y subs
    - Canales mas vistos esta semana
    - Top 10 likes
    - Top 10 more coment

	

    datetime.datetime.today().weekday()
"""


def start(topic):

    conf = SparkConf().setMaster('local').setAppName('StreamData')
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

    #En dataframe juntaremos los datos de todos los paises
    # para poder sacar estadisticas globales
    ruta = '../spark/dataStreaming.csv'
    dataframe = pd.read_csv(ruta)
   
    #topic not in ["most_view", "top_revelation", "best_day","most_liked","most_comented"]):
    df = pd.read_csv(ruta)

    if (topic == "most_view"):
        year_statistics(dataframe)
    elif (topic == "top_revelation"):
        month_statistics(dataframe)
    elif (topic == "best_day"):
        month_statistics(dataframe)
    elif (topic == "most_liked"):
        month_statistics(dataframe)
    else:
        global_category(dataframe)

def most_view(df):

  categories = df.groupBy("category_id").count()
  #mapping_expr = create_map([lit(x) for x in chain(*category_list.items())])
  #categories = categories.withColumn('category_id', mapping_expr[categories['category_id']])
  categories = categories.filter(categories.category_id. isNotNull())
  categories.show()

  return df

def top_revelation(df):

  ruta = '../data/'+countrie+'videos.csv'
  df = sqlContext.read.csv(ruta, header = True, sep=',',schema=struct1,encoding='utf-8')

  categories = df.groupBy("category_id").count()
  mapping_expr = create_map([lit(x) for x in chain(*category_list.items())])
  categories = categories.withColumn('category_id', mapping_expr[categories['category_id']])
  categories = categories.filter(categories.category_id. isNotNull())
  categories.show()

  return df



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

def most_view(dataframe):

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
    #helpRegionCode = 'Region code for the youtube videos, by default ALL.\nPossible regions:\nCA: Canada,\n\tDE: Alemania,\n\tFR: Francia,\n\tGB: Reino Unido,\n\tIN: India,\n\tJP: Japon,\n\tKR: Korea,\n\tMX: Mexico,\n\tRU: Rusia,\n\tUS: Estados Unidos'
    parser.add_argument(
        "topic", help="Available options: 1. most_view\n 2. top_revelation\n 3. best_day\n 4. most_liked\n 5. most_comented", default="ALL")
    args = parser.parse_args()
    # END OF ARGUMENT PARSER

    topic = args.topic.upper()

    if (topic not in ["most_view", "top_revelation", "best_day","most_liked","most_comented"]):
        sys.exit(1)
        pass

    start(topic)
