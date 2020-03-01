package br.semantix.desafios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class Utils extends Serializable {

  @transient lazy val log : Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  private def getSession(appName:String):SparkSession = {
//    System.setProperty("hadoop.home.dir", "C:\\Users\\Joao Tieles\\Documents\\Hadoop-master\\bin")
    System.setProperty("hadoop.home.dir", "C:\\Users\\Joao Tieles\\Documents\\hadoop-2.8.5\\bin")

    SparkSession
      .builder()
      .master("local")
      .appName(appName)
      .getOrCreate()
  }

  private def getSchema() = {

    val fields = List(
      StructField("Host", StringType, nullable = false),
      StructField("Timestamp", StringType, nullable = false),
        StructField("Requisicao", StringType, nullable = false),
        StructField("Codigo", StringType, nullable = false),
        StructField("Bytes", StringType, nullable = false)
    )
    fields
  }

  private def getFile(ss:SparkSession, filePath:String) = {

    val fields = getSchema()
    val rdd = ss.sparkContext.textFile(filePath)
      .map(linha => formatRow(linha))

    val df = ss.createDataFrame(rdd, StructType(fields))
    df.printSchema()
    df.show(2)
    df
  }

  private def formatRow(linha:String): Row ={
    try {
      val row = Row.fromSeq(
        new String(linha.split(" - -")(0) + "|" +
          linha.split("\\[")(1).split(" \"")(0).replace("]", "") + "|" +
          linha.split("\"")(1) + "|" +
          linha.split("\"")(2).split(" ")(1) + "|" +
          linha.split(" ").last).split("\\|"))
      row
    } catch {
      case e: ArrayIndexOutOfBoundsException => log.error(s"Má formatação da linha /n ${e.getStackTrace}")
        Row.fromSeq("null null null null null".split(" "))
    }
  }

  def getLogs(appName:String):Dataset[Row] ={
    val sparkSession = getSession(appName)
    val df_aug = getFile(sparkSession, "src/main/resources/access_log_Aug95.log")
    val df_jul = getFile(sparkSession, "src/main/resources/access_log_Jul95.log")
    df_jul.union(df_aug)
  }
}
