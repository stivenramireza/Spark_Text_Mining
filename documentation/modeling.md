# Proyecto 3 (Big Data) - Minería de Texto con Spark

## Integrantes 

- Stiven Ramírez Arango - sramir70@eafit.edu.co - Video Sustentación: 
- Sebastián Ospina Cabarcas - sospin26@eafit.edu.co - Video Sustentación: https://bit.ly/2J00KnF
- Camilo Suaza Gallego - csuazag@eafit.edu.co - Video Sustentación: https://youtu.be/OR5WCcMA_ls

# Modeling

<p align="center"> <img src="http://crisp-dm.eu/wp-content/uploads/2013/03/Modeling.jpg"> </p>

En esta cuarta fase se construyen los modelos del **índice invertido** y **agrupamiento de noticias por similaridad**.

## 1. Índice Invertido

El índice invertido es una estructura de datos que contiene la siguiente estructura:

| Palabra          | Lista de noticias (5 noticias de mayor frecuencia)   |
| ------------- |:-------------:|
| Palabra 1      | [(news1,f1), (news11,f11),(news4,f4),(news10,f10), … (news50,f50) ] |
| Palabra 2         | [(news2,f2), (news14,f14),(news1,f1),(news20,f20), … (news3,f3) ]      |
|  Palabra 3      | [(news50,f50), (news1,f1),(news11,f11),(news21,f21), … (news2,f2) ]      |
| ... | ... |
| Palabra N| ||

En el índice invertido se tiene la frecuencia de cada palabra en el título + descripción, donde por cada palabra que se ingrese por teclado en el notebook de **Databricks**, se lista en orden descendente por frecuencia de palabra en el contenido <título> de la noticia, las noticias más relevantes. Se listan las 5 de mayor frecuencia <frec, news_id, tittle>.


- **frec**: Frecuencia de la palabra en la noticia < id > (incluye título y descripción).
- **id**: Id de la noticia.
- **tittle**: Título de la noticia.

### 1.1 Diseño

En esta etapa se deben tener los datos limpios, y esto ya se llevó a cabo en la fase anterior de **Data Preparation**.

Se selecciona los datos que se van a trabajar de la siguiente manera:

- id_noticia
- título_noticia_limpio
- contenido_noticia_limpio

Para ello realizamos una consulta SQL que muestre los datos a procesar:

```sql
SELECT * 
FROM dataCleaned
LIMIT 5; 
```

Se obtiene la siguiente tabla:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57049746-09859680-6c3f-11e9-8bb5-99ad62ba21c6.png"> </p>

### 1.2 Desarrollo

Se lee el dataframe con pyspark y se pone en el contexto de Python:

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
dataFrame = sqlContext.sql("select id,tittle,content from dataCleaned")
```

Se convierte el dataframe a RDD (Resilient Distributed Dataset) para llevar a cabo actiones y transformaciones en dicho dataset:

```python
RDD = dataFrame.rdd
RDD = RDD.map(lambda line: (str(line[0]), str(line[1]), list(line[2])))
```
Luego se procede a realizar la estructura del índice invertido:

```python
from operator import itemgetter

tuplas = RDD.flatMap(lambda line: [(word, (line[0], line[1])) for word in line[2]]) # [(word,tittle_article),(word, tittle_article))] 
count = tuplas.map(lambda line : ((line), 1)) # [((word, tittle_article), 1), ((word, tittle_article), 1)]
emmit = count.reduceByKey(lambda accum, n: accum + n) # [((word, (id, tittle_article)), n), ((word, (id, tittle_article)), n)]
map_emmit = emmit.map(lambda line: (line[0][0], (line[0][1][0], line[0][1][1], line[1]))) # ((w, (id, tittle_article)), n) => (w, (id, tittle_article, n)) 
group = map_emmit.groupByKey() # (w, (id, tittle_article, n))
inverted_index_rdd = group.map(lambda line: (line[0], sorted(list(line[1]), key=itemgetter(2), reverse=True)[:5])) # (w, [id, tittle_article, n])
```
Y con el objetivo de realizar una búsqueda rápida con una complejidad **O(1)**, se convierte el RDD con el índice invertido en un diccionario que contiene como **key** la palabra a buscar y como **value** la lista de las 5 noticias de mayor frecuencia en relación con dicha palabra:

```python
diccionario = inverted_index_rdd.collectAsMap()
```
Finalmente, se usa los widgets de **Databricks** para ingresar la palabra a buscar y se construye el buscador con base en el índice invertido:

```python
dbutils.widgets.text(name='widget_01', defaultValue='', label = 'Entrar la palabra: ')
palabra = dbutils.widgets.get(name='widget_01')
palabra_buscar = palabra.lower().strip()
indice_invertido = diccionario[palabra_buscar]
for i in indice_invertido:
  print("{}, {}, {}".format(i[2], i[0], i[1]))
```

### 1.3 Pruebas

Se verifica que el índice invertido se haya construído correctamente:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57196989-8c506f00-6f27-11e9-95d2-58a9849096cb.png"> </p>

### 1.4 Instalación

Para llevar a cabo este proceso se debe estar trabajando con las siguientes herramientas tecnológicas:

- Apache Spark.
- Databricks.
- Python.

En general, no hay que instalar programas pues el notebook de **Databricks** se encuentra en un ambiente **Spark** y permite la implementación en el lenguaje de programación **Python**.

### 1.5 Ejecución

Se ingresa una palabra por el notebook de **Databricks**:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57197034-f6691400-6f27-11e9-8915-30050026f775.png"> </p>

Y se obtiene el siguiente resultado:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57197057-30d2b100-6f28-11e9-8ea8-1242042042dc.png"> </p>

## 2. Agrupamiento de Noticias por Similaridad

Antes de realizar el agrupamiento de noticias por similaridad, es necesario construir una estructura de datos que almacene el id de la noticia y con base en ese se obtenga el top 10 de palabras más frecuentes por noticia:

| Noticia          | Top 10 de palabras más frecuentes por noticia (sin stop-words)   |
| ------------- |:-------------:|
| News1 | [(word1,f1), (word11,f11),(word4,f4),(word10,f10), … (word50,f50) ] |
| News2 | [(word2,f2), (word14,f14),(word1,f1),(word20,f20), … (word3,f3) ] |
| News3 | [(word50,f50), (word1,f1),(word11,f11),(word21,f21), … (word2,f2) ] |
| ... | ... |
| NewsM | ||

Esto con el fin de que por cada **news_id** que se ingrese por teclado en el notebook de **Databricks**, se liste en orden descendente de similitud las 5 noticias más relacionadas con dicho **id**.

- **id**: Id de la noticia.
- **tittle**: Título de la noticia.
- **list_news_id**: Lista de noticias relacionadas.

Posteriormente se realiza el agrupamiento con base en un modelo conocido como **Doc2Vec**.

Este modelo consiste en que dado un número de documentos, se construye un vector de características que los describen y que tiene en cuenta el contexto que rodea a las palabras:

<p align="center"> <img src="https://cdn-images-1.medium.com/max/1600/1*YfOv1_8tmTiahgbpEt5LCw.png"> </p>

Y así sucesivamente hasta conseguir un resultado como este:

<p align="center"> <img src="https://irenelizihui.files.wordpress.com/2016/07/13950777_843664199102483_1888567216_o.jpg?w=656"> </p>

### 2.1 Diseño

En esta etapa se deben tener los datos limpios, y esto ya se llevó a cabo en la fase anterior de **Data Preparation**.

Se selecciona los datos que se van a trabajar de la siguiente manera:

- id_noticia
- título_noticia_limpio
- contenido_noticia_limpio

Para ello realizamos una consulta SQL que muestre los datos a procesar:

```sql
SELECT * 
FROM dataCleaned
LIMIT 5; 
```

Se obtiene la siguiente tabla:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57049746-09859680-6c3f-11e9-8bb5-99ad62ba21c6.png"> </p>

### 2.2 Desarrollo

Se preparan los datos para el clustering y para ello se construye una estructura que permita agrupar **id_noticia** con su título y sus palabras más relacionadas:

```python
from operator import itemgetter

rdd1 = RDD.map(lambda line: ((line[0], line[1]), line[2])) # [ ((id, tittle),list_content), ((id, tittle),list_content) ]
rdd2 = rdd1.flatMap(lambda line: [(line[0], word) for word in line[1]]  ) #[ ((id, tittle_article), word), ((id, tittle_article), word)) ]
rdd3 = rdd2.map(lambda line : (line, 1)) # [ (((id, tittle_article), word), 1), ((word, tittle_article), 1) ]
rdd4 = rdd3.reduceByKey(lambda accum, n: accum + n) #  [ ((((id, tittle_article), word), n), (((id, tittle_article), word), n)) ]
rdd5 = rdd4.map(lambda line: ((line[0][0][0], line[0][0][1]),(line[0][1],line[1])))#  [ ((id,tittle),(word, n)) ]
rdd6 = rdd5.groupByKey()
rdd7 = rdd6.map(lambda line: (line[0], sorted(list(line[1]), key=itemgetter(1), reverse=True)[:10]))
```
Luego se convierte a diccionario para realizar una búsqueda más rápido y con una complejidad **O(1)** y se obtienen las **keys**:

```python
diccionario2 = rdd7.collectAsMap()
diccionario2.keys()
```
Posteriormente, se realiza el agrupamiento de las noticias concatenando las palabras más frecuentes:

```python
def concatenar(lista_palabras):
  cadena = []
  for i in lista_palabras:
    word = i[0]
    times = i[1]
    for j in range(times):
      cadena.append(word)
  return cadena

ids = rdd7.map(lambda line: line[0][0]).collect()
tittles = rdd7.map(lambda line: line[0][1]).collect()
texts = rdd7.map(lambda line: concatenar(line[1])).collect()
```
Se entrena el modelo **Doc2Vec** y se guarda su resultado de entrenamiento para usarlo en el buscador:

```python
dbutils.library.installPyPI('gensim')
dbutils.library.installPyPI('tqdm')
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import utils
from tqdm import tqdm

train_documents = [TaggedDocument(words=doc, tags=[ids[i]]) for i, doc in enumerate(texts)]
max_epochs = 20
vec_size = 300
alpha = 0.025

model = Doc2Vec(dm =1,
                size=vec_size,
                min_count=1,
                alpha=alpha,
                min_alpha=0.00025,
                workers=4)

model.build_vocab(train_documents)
train_documents = utils.shuffle(train_documents)

for epoch in range(max_epochs):
    print('iteration {0}'.format(epoch))
    model.train(train_documents,
                total_examples=model.corpus_count,
                epochs=model.iter)
    # decrease the learning rate
    model.alpha -= 0.0002
    # fix the learning rate, no decay
    model.min_alpha = model.alpha

model.save("d2v.model")
```
Finalmente, se realiza el buscador de una noticia por **id** y se obtiene sus similares:

```python
from gensim.models.doc2vec import Doc2Vec

model= Doc2Vec.load("d2v.model")
id_busqueda = '20658'
similar_doc = model.docvecs.most_similar(id_busqueda)
lista_final = []

for i in similar_doc:
  lista_final.append(i[0])

indice = ids.index(id_busqueda)
print("{}, {}, {}".format(ids[indice],tittles[indice],lista_final))
```

### 2.3 Pruebas

Se verifica que se haya construído correctamente la estructura para el clustering de noticias:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57197100-bfdfc900-6f28-11e9-8389-03723cfa3caa.png"> </p>

### 2.4 Instalación

Para llevar a cabo este proceso se debe estar trabajando con las siguientes herramientas tecnológicas:

- Apache Spark.
- Databricks.
- Python.

En general, no hay que instalar programas pues el notebook de **Databricks** se encuentra en un ambiente **Spark** y permite la implementación en el lenguaje de programación **Python**.

### 2.5 Ejecución

Se ingresa el **id** de la noticia a buscar:


<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57197238-9aec5580-6f2a-11e9-8d73-17997ca5c066.png"> </p>

Y se obtiene el siguiente resultado:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57197317-7644ad80-6f2b-11e9-961a-7a946c6e48a4.png"> </p>
