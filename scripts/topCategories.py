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

"""

	Este srcript nos permite sacar estadísticas sobre 
	categorías de un pais o de todos.
	Los paises son: 
		Canada
		Alemania
		Francia
		Reino Unido
		India
		Japon
		Korea
		Mexico
		Rusia
		Estados Unidos
	
"""

def start(options):

	conf = SparkConf().setMaster('local').setAppName('TOP_Category')
	sc = SparkContext(conf = conf)
	sqlContext = SQLContext(sc)
	
	#diccionario con las abreviaturas y el nombre completo del pais

	countries = {'CA':'Canada','DE':'Alemania','FR':'Francia',
	           'GB':'Reino Unido','IN':'India','JP':'Japon',
	           'KR':'Korea','MX':'Mexico','RU':'Rusia',
	           'US':'Estados Unidos'}
			   
	#Estructura para definir las columnas y tipos de nuestro sqlContext

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
							StructField("video_error_or_removed", StringType(), True),
							StructField("description", StringType(), True)])

	prefijos = countries.keys()

	with open('../data/CA_category_id.json') as json_file:
	    data = json.load(json_file)
	    store_list = dict()
	    for item in data['items']:
	    	index = int(item['id'])
	        store_list[index] = item['snippet']['title']
	    print (store_list.values())

	#console_all(prefijos,store_list,sqlContext,struct1)
	graficas_all(prefijos,store_list,countries)

def console_all(prefijos,category_list,sqlContext,struct1):

	dataframes = dict() 

	for countrie in prefijos:
		ruta = '../data/'+countrie+'videos.csv'
		dataframes[countrie] =  console_pais(countrie,category_list,struct1,sqlContext)

	return dataframes

def console_pais(countrie,category_list,struct1,sqlContext):

	ruta = '../data/'+countrie+'videos.csv'
	df = sqlContext.read.csv(ruta, header = True, sep=',',schema=struct1,encoding='utf-8')

	categories = df.groupBy("category_id").count()
	mapping_expr = create_map([lit(x) for x in chain(*category_list.items())])
	categories = categories.withColumn('category_id', mapping_expr[categories['category_id']])
	categories = categories.filter(categories.category_id. isNotNull())
	categories.show()

	return df
def graficas_pais(countrie,category_list,countries):

	ruta = '../data/'+countrie+'videos.csv'
	print(ruta)
	df= pd.read_csv(ruta)
	df = df.replace({"category_id": category_list})
	f=sb.catplot('category_id',data = df,kind = "count", aspect = 3,hue_order =category_list.values(),order=category_list.values())
	f.set_xticklabels(fontsize = 11)
	plt.xlabel("Categoria")
	plt.ylabel("Numero de videos en tendencias")
	plt.title("Numero de videos en tendencias por categoria en "+ countries[countrie])
	fig = plt.gcf()
	fig.set_size_inches(30, 10.5)
	fig.savefig('categoriaTop'+countrie+'.png', dpi=100)

	return df

def graficas_all(prefijos,category_list,countries):

	dataframes = dict() 

	for countrie in prefijos:
		dataframes[countrie] = graficas_pais(countrie,category_list,countries)

	return dataframes	

if __name__ == "__main__":
    start("hola")
    