# Leer de la vista creada en scala
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)
dataFrame = sqlContext.sql("select id,tittle,content from dataCleaned")

# Convert DataFrame Python to RDD Python
RDD = dataFrame.rdd
RDD = RDD.map(lambda line: (str(line[0]), str(line[1]), list(line[2])))

#Indice Invertido con Wordcount
from operator import itemgetter

tuplas = RDD.flatMap(lambda line: [(word, (line[0], line[1])) for word in line[2]]) # [(word,tittle_article),(word, tittle_article))] 
count = tuplas.map(lambda line : ((line), 1)) # [((word, tittle_article), 1), ((word, tittle_article), 1)]
emmit = count.reduceByKey(lambda accum, n: accum + n) # [((word, (id, tittle_article)), n), ((word, (id, tittle_article)), n)]
map_emmit = emmit.map(lambda line: (line[0][0], (line[0][1][0], line[0][1][1], line[1]))) # ((w, (id, tittle_article)), n) => (w, (id, tittle_article, n)) 
group = map_emmit.groupByKey() # (w, (id, tittle_article, n))
inverted_index_rdd = group.map(lambda line: (line[0], sorted(list(line[1]), key=itemgetter(2), reverse=True)[:5])) # (w, [id, tittle_article, n])
diccionario = inverted_index_rdd.collectAsMap()

# Buscador con base en el índice invertido
dbutils.widgets.text(name='widget_01', defaultValue='', label = 'Entrar la palabra: ')
palabra = dbutils.widgets.get(name='widget_01')
palabra_buscar = palabra.lower().strip()
indice_invertido = diccionario[palabra_buscar]
for i in indice_invertido:
  print("{}, {}, {}".format(i[2], i[0], i[1]))
  
# Deployment del índice invertido
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("id", StringType(), True),
    StructField("titulo", StringType(), True),
    StructField("frecuencia", IntegerType(), True)
])
DF = sqlContext.createDataFrame(indice_invertido, schema)
display(DF.select("id","frecuencia"))
