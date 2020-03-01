package br.semantix.desafios

class Urls404 {

//  3. Os 5 URLs que mais causaram erro 404.
  def getUrls404(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("urls 404")

    val count = logs.select("Requisicao","Codigo").filter("Codigo = '404'").groupBy("Requisicao").count().as("count")
    count.sort(count.col("count").desc).limit(5).show()

//      +--------------------+-----+
//      |          Requisicao|count|
//      +--------------------+-----+
//      |GET /pub/winvn/re...| 2004|
//      |GET /pub/winvn/re...| 1732|
//      |GET /shuttle/miss...|  682|
//      |GET /shuttle/miss...|  426|
//      |GET /history/apol...|  384|
//      +--------------------+-----+

    logs.createOrReplaceTempView("falhas")
    logs.sqlContext.sql("select Requisicao, count(Codigo) total_falhas from falhas where Codigo = '404' group by  Requisicao order by count(Codigo) desc").show()
//      +--------------------+------------+
//      |          Requisicao|total_falhas|
//      +--------------------+------------+
//      |GET /pub/winvn/re...|        2004|
//      |GET /pub/winvn/re...|        1732|
//      |GET /shuttle/miss...|         682|
//      |GET /shuttle/miss...|         426|
//      |GET /history/apol...|         384|
//      +--------------------+------------+

  }

}
