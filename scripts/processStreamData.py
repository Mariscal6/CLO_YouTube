#!/usr/bin/python

from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import string
import pandas as pd
import numpy as np
import json
from pyspark.sql.functions import col, create_map, lit, date_format
from itertools import chain
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import matplotlib.patches as mpatches
import seaborn as sb
import sys

"""

	Este srcript nos permite sacar estadisticas de
  Espana en tiempo real.
		- Dias de la semana con mas visitas
    - Canales con mayor proporcion de views y subs
    - Top 10 likes
    - Top 10 more coment

"""


def start(topic):

    conf = SparkConf().setMaster('local').setAppName('StreamData')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Estructura para definir las columnas y tipos de nuestro sqlContext

    struct1 = StructType([StructField("videoID", StringType(), True),
                          StructField("Published_at", TimestampType(), True),
                          StructField("Video_title", StringType(), True),
                          StructField("Description", StringType(), True),
                          StructField("category_id", IntegerType(), True),
                          StructField("Channel_title", StringType(), True),
                          StructField("Tags", StringType(), True),
                          StructField("View_count", IntegerType(), True),
                          StructField("Like_count", IntegerType(), True),
                          StructField("Dislike_count", IntegerType(), True),
                          StructField("Favorite_count", IntegerType(), True),
                          StructField("Comment_count", IntegerType(), True)
                         ])

    print ("------------------------------")
    #En dataframe juntaremos los datos de todos los paises
    # para poder sacar estadisticas globales
    ruta = '../spark/dataStreaming.csv'
    #dataframe = pd.read_csv(ruta,sep = ';')
    dataframe = sqlContext.read.csv(ruta, header = True, sep=';',encoding='utf-8')
    dataframe.printSchema()
    #topic not in ["most_view", "top_revelation", "best_day","most_liked","most_comented"]):
    #df = pd.read_csv(ruta)

    if (topic == "MOST_VIEW"):
        most_view(dataframe,sqlContext)
    elif (topic == "TOP_REVELATION"):
        month_statistics(dataframe)
    elif (topic == "BEST_DAY_OF_WEEK"):
        best_day_of_week(dataframe,sqlContext)
    elif (topic == "MOST_LIKED"):
        most_liked(dataframe,sqlContext)
    elif (topic == "MOST_COMMENTED"):
        most_comented(dataframe,sqlContext)
    else:
        global_category(dataframe)

def most_view(df,sqlContext):
  df.createOrReplaceTempView("videos")
  df_final=sqlContext.sql("SELECT V.Video_title,V.View_count FROM videos as V ORDER BY BIGINT(V.View_count) DESC")
  df_final.show(10,False)

  return df

def most_liked(df,sqlContext):
  df.createOrReplaceTempView("videos")
  df_final=sqlContext.sql("SELECT V.Video_title,V.Like_count FROM videos as V ORDER BY BIGINT(V.Like_count) DESC")
  df_final.show(10,False)

  pass

def most_comented(df,sqlContext):
  df.createOrReplaceTempView("videos")
  df_final=sqlContext.sql("SELECT V.Video_title,V.Comment_count FROM videos as V ORDER BY BIGINT(V.Comment_count) DESC")
  df_final.show(10,False)

  pass

def best_day_of_week(df,sqlContext):
  df.createOrReplaceTempView("videos")
  df = sqlContext.sql("SELECT V.Video_title,timestamp(V.Published_at),V.View_count FROM videos as V")
  df = df.select('Video_title','Published_at','View_count',  date_format('Published_at', 'E').alias('WeekDay'))
  df.createOrReplaceTempView("videos_dia")
  df = sqlContext.sql("SELECT V.WeekDay, sum(V.View_count) AS vitas_en_dia FROM videos_dia as V GROUP BY V.WeekDay ORDER BY vitas_en_dia DESC")
  #df_final.show(10,False)
  #df2 = df.select("Published_at").rdd.flatMap(lambda x: x + ("anything", )).toDF()
  #df.Published_at = df.Published_at.apply(lambda x: x.weekday())
  df.show()
  #df.printSchema()
  pass

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
        "topic", help="Available options: 1. most_view\n 2. top_revelation\n 3. best_day_of_week\n 4. most_liked\n 5. most_comented", default="ALL")
    args = parser.parse_args()
    # END OF ARGUMENT PARSER

    topic = args.topic.upper()

    if (topic not in ["MOST_VIEW", "TOP_REVELATION", "BEST_DAY_OF_WEEK","MOST_LIKED","MOST_COMENTED"]):
        sys.exit(1)
        pass

    start(topic)
