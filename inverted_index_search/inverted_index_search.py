# Leer de la vista creada en scala
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
dataFrame = sqlContext.sql("select id,tittle,content from dataCleaned")
dataFrame.show(n=3)

# Convert DataFrame Python to RDD Python
RDD = dataFrame.rdd
RDD = RDD.map(lambda line: (str(line[0]), str(line[1]), list(line[2]) ))
RDD.take(1)

#Indice Invertido con Wordcount
from operator import itemgetter
tuplas = RDD.flatMap(lambda line: [(word, (line[0], line[1])) for word in line[2]] ) #[ (word,tittle_article),(word, tittle_article)) ] 
count = tuplas.map(lambda line : ((line), 1)) # [ ((word, tittle_article), 1), ((word, tittle_article), 1) ]
emmit = count.reduceByKey(lambda accum, n: accum + n) #  [ ((word, (id, tittle_article)), n), ((word, (id, tittle_article)), n) ]
map_emmit = emmit.map(lambda line: (line[0][0], (line[0][1][0], line[0][1][1], line[1])))#  ((w, (id, tittle)), n) => (w, (id, tittle, n)) 
group = map_emmit.groupByKey()
inverted_index_rdd = group.map(lambda line: (line[0], sorted(list(line[1]), key=itemgetter(2), reverse=True)[:5]))
inverted_index_rdd.take(5)

# Buscador con base en el índice invertido
dbutils.widgets.text(name='widget_01', defaultValue='', label = 'Entrar la palabra: ')
palabra = dbutils.widgets.get(name='widget_01')
palabra_buscar = palabra.lower()
search_keywords = set(palabra_buscar.split()) ## Split la palabra
filtrado = inverted_index_rdd.filter(lambda line: line[0] in search_keywords) # Filtrar la palabra en el RDD 
result_rdd = filtrado.flatMap(lambda line: list(line[1])) # Obtener la lista[1]
for line in result_rdd.collect():
  print (str(line[2]) + "," + str(line[0]) + "," + str(line[1]))