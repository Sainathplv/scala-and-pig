// Databricks notebook source
import org.apache.spark.sql.SparkSession
 
val spark = SparkSession
  .builder()
  .appName("6332")
  .getOrCreate()

// COMMAND ----------

case class MatM(i: Int, j: Int, v: Double)
val MatMRDD = sc.parallelize(Seq(MatM(1,1,-2.0),
                                 MatM(0,0,5.0),
                                 MatM(2,2,6.0),
                                 MatM(0,1,-3.0),
                                 MatM(3,2,7.0),
                                 MatM(0,2,-1.0),
                                 MatM(1,0,3.0),
                                 MatM(1,2,4.0),
                                 MatM(2,0,1.0),
                                 MatM(3,0,-4.0),
                                 MatM(2,1,0.0),
                                 MatM(3,1,2.0)))
val MatMDF = MatMRDD.toDF
MatMDF.printSchema()

// COMMAND ----------

MatMDF.show()

// COMMAND ----------

MatMDF.createOrReplaceTempView("MatM")

// COMMAND ----------

case class MatN(j: Int, k: Int, v: Double)
val MatNRDD = sc.parallelize(Seq(MatN(1,0,3.0),
                                 MatN(0,0,5.0),
                                 MatN(1,2,-2.0),
                                 MatN(2,0,9.0),
                                 MatN(0,1,-3.0),
                                 MatN(0,2,-1.0),
                                 MatN(1,1,8.0),
                                 MatN(2,2,0.0),
                                 MatN(2,1,4.0)))

// COMMAND ----------

val MatNDF = MatNRDD.toDF
MatNDF.printSchema()

// COMMAND ----------

MatNDF.show()

// COMMAND ----------

MatNDF.createOrReplaceTempView("MatN")

// COMMAND ----------

val Mult1DF = spark.sql("SELECT MatM.i, MatN.k, MatM.v * MatN.v AS product FROM MatM JOIN MatN ON MatM.j = MatN.j")


// COMMAND ----------

Mult1DF.show(100)


// COMMAND ----------

/*val MultDF = MatMDF.join(MatNDF, MatMDF("j") === MatNDF("j"))*/

// COMMAND ----------

/*MultDF.show*/


// COMMAND ----------

Mult1DF.createOrReplaceTempView("Mult")

// COMMAND ----------

val RESULTDF = spark.sql("SELECT Mult.i, Mult.k, SUM(product) AS result FROM Mult GROUP BY Mult.i, Mult.k ORDER BY Mult.i, Mult.k")

// COMMAND ----------

RESULTDF.show(100)

// COMMAND ----------


