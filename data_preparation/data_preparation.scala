// Cargar datos en un dataframe
val articles1 = spark.sql("select id,title,content from articles1_csv where id IS NOT NULL and title IS NOT NULL and content IS NOT NULL ")
val articles2 = spark.sql("select id,title,content from articles2_csv where id IS NOT NULL and title IS NOT NULL and content IS NOT NULL")
val articles3 = spark.sql("select id,title,content from articles2_csv where id IS NOT NULL and title IS NOT NULL and content IS NOT NULL")

val dataSet = articles3.union(articles2.union(articles1))

// Tokenizar el contenido
import org.apache.spark.ml.feature.Tokenizer
val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("contentWords")
val contentTokenized = tokenizer.transform(dataSet)

// Clase info
case class info(id: String, tittle: String, content: List[String])

// Convertir de dataframe a RDD
import scala.collection.mutable.WrappedArray

val contentRdd = contentTokenized.rdd.map((row) => info(row.getAs[String](0),
                                                        row.getAs[String](1),
                                                        row.getAs[WrappedArray[String]](3).toList.map(_.toLowerCase)))

// Remover caracteres en blanco, especiales y stop words
import org.apache.spark.ml.feature.StopWordsRemover
val stopWords = new StopWordsRemover().getStopWords

def isOrdinary(s: String): Boolean = s.forall(_.isLetterOrDigit)

val dataCleaned = contentRdd.map((data: info) => info(data.id,
                                                      data.tittle,
                                                      data.content.filter(
                                                        (x: String) => (x.size >= 1) && isOrdinary(x) && !stopWords.contains(x))
                                                     )).filter(x => !x.content.isEmpty)

dataCleaned.take(1)

// Crear vista desde scala
import sqlContext.implicits._

dataCleaned.toDF().createOrReplaceTempView("dataCleaned")