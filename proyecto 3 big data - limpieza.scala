// Databricks notebook source
// DBTITLE 1,Cargar datos en un dataframe
// MAGIC %scala
// MAGIC 
// MAGIC 
// MAGIC val dataSet = spark.sql("select id,title,content from articles1_csv ")

// COMMAND ----------

// DBTITLE 1,Tokenizar el contenido
// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.ml.feature.Tokenizer
// MAGIC val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("contentWords")
// MAGIC val contentTokenized = tokenizer.transform(dataSet)

// COMMAND ----------

// DBTITLE 1,Clase info 
// MAGIC %scala 
// MAGIC 
// MAGIC case class info(id: String, tittle: String, content: List[String])

// COMMAND ----------

// DBTITLE 1,Convertir de dataframe a RDD
// MAGIC %scala
// MAGIC 
// MAGIC import scala.collection.mutable.WrappedArray
// MAGIC 
// MAGIC val contentRdd = contentTokenized.rdd.map((row) => info(row.getAs[String](0),
// MAGIC                                                         row.getAs[String](1),
// MAGIC                                                         row.getAs[WrappedArray[String]](3).toList.map(_.toLowerCase)))
// MAGIC 
// MAGIC //contentFiltered.take(1).foreach((row) => println(row(3)))

// COMMAND ----------

// DBTITLE 1,Remover caracteres en blanco, especiales y stop words
// MAGIC %scala 
// MAGIC 
// MAGIC import org.apache.spark.ml.feature.StopWordsRemover
// MAGIC val stopWords = new StopWordsRemover().getStopWords
// MAGIC 
// MAGIC def isOrdinary(s:String): Boolean = s.forall(_.isLetterOrDigit)
// MAGIC 
// MAGIC val dataCleaned = contentRdd.map(_.content.filter((x: String) => !x.isEmpty && isOrdinary(x) && !stopWords.contains(x)))
