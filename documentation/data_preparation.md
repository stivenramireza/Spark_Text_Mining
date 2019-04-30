# Proyecto 3 (Big Data) - Minería de Texto con Spark

    Stiven Ramírez Arango - sramir70@eafit.edu.co
    Sebastián Ospina Cabarcas - sospin26@eafit.edu.co
    Camilo Suaza Gallego - csuazag@eafit.edu.co

# Data Preparation

<p align="center"> <img src="http://crisp-dm.eu/wp-content/uploads/2013/03/Data-Preparation.jpg"> </p>

En esta tercera fase se construye el dataset con el cual se va a trabajar para la limpieza de datos.

## 1. Diseño

Se selecciona los datos que se van a trabajar de la siguiente manera:

- id_noticia
- título_noticia
- contenido_noticia

Para ello realizamos una consulta SQL que muestre los datos a procesar:

```sql
SELECT id,title,content 
FROM articles1_csv 
WHERE id IS NOT NULL 
AND title IS NOT NULL 
AND content IS NOT NULL 
LIMIT 5;
```
Se obtiene la siguiente tabla:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/56938891-5e53d080-6aca-11e9-8c5f-455fb9f0861c.png"> </p>

## 2. Desarrollo

Se cargan los datos en un dataframe para ponerlos en el contexto de Spark con Scala:

```scala
val articles1 = spark.sql("select id,title,content from articles1_csv where id IS NOT NULL and title IS NOT NULL and content IS NOT NULL ")
val articles2 = spark.sql("select id,title,content from articles2_csv where id IS NOT NULL and title IS NOT NULL and content IS NOT NULL")
val articles3 = spark.sql("select id,title,content from articles2_csv where id IS NOT NULL and title IS NOT NULL and content IS NOT NULL")
val dataSet = articles3.union(articles2.union(articles1))
```

Posteriormente se tokeniza su contenido para realizar una correcta transformación del dataset de **csv** a **dataframe**:

```scala
import org.apache.spark.ml.feature.Tokenizer

val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("contentWords")
val contentTokenized = tokenizer.transform(dataSet)
```

Se crea un objeto **info** que tendrá la información que debe contener un RDD: **id**, **título** y **contenido**:

```scala
case class info(id: String, tittle: String, content: List[String])
```

Se convierte el **dataframe** a **RDD (Resilient Distributed Dataset)** para llevar a cabo **actiones** y **transformaciones** en dicho dataset:

```scala
import scala.collection.mutable.WrappedArray

val contentRdd = contentTokenized.rdd.map((row) => info(row.getAs[String](0),
                                                        row.getAs[String](1),       
                                                        row.getAs[WrappedArray[String]](3).toList.map(_.toLowerCase)))
```

Con el **RDD** construído, se procede a remover los caracteres en blanco, las palabras de longitud 1, los caracteres en blanco y los stop words del inglés:

```scala
import org.apache.spark.ml.feature.StopWordsRemover

val stopWords = new StopWordsRemover().getStopWords
def isOrdinary(s: String): Boolean = s.forall(_.isLetterOrDigit)
val dataCleaned = contentRdd.map((data: info) => info(data.id,
                                                      data.tittle,
                                                      data.content.filter(
                                                        (x: String) => (x.size >= 1) && isOrdinary(x) && !stopWords.contains(x))
                                                     )).filter(x => !x.content.isEmpty)
```
Se muestran los **stop words** que fueron removidos correctamente:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/56939163-66147480-6acc-11e9-8be7-8ebc9549d7ef.png"> </p>

Se convierte el **RDD** a **dataframe** con los datos limpios:

```scala
import sqlContext.implicits._

dataCleaned.toDF().createOrReplaceTempView("dataCleaned")
```

Se realiza una consulta SQL a dicho dataframe:

```sql
SELECT * from dataCleaned limit 5;
```

Y se observa el resultado de dicha consulta con el dataframe completamente limpio:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/56939298-35810a80-6acd-11e9-8792-c3bc12416058.png"> </p>

## 3. Pruebas

Se verifica que los stop words sean eliminados correctamente y se muestra el RDD limpio:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/56939374-93155700-6acd-11e9-95b2-fbd8c2717622.png"> </p>

## 4. Instalación

Para llevar a cabo este proceso se debe estar trabajando con las siguientes herramientas tecnológicas:

- Apache Spark.
- Databricks.
- Scala.

En general, no hay que instalar programas pues el notebook de **Databricks** se encuentra en un ambiente **Spark** y permite la implementación en el lenguaje de programación **Scala**.

## 5. Ejecución

Basta con hacer click en el botón **Run** donde se encuentra la porción de código que muestra los datos limpios, así:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/56939492-49793c00-6ace-11e9-96e2-d1940b360c71.png"> </p>

## 6. Video
[link](https://drive.google.com/file/d/1xhLoMuZs7Z4egCrR2knEhcNu3ZHUADBR/view?usp=sharing)
