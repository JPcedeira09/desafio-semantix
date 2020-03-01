package br.semantix.desafios

class TotalDeBytes {

//  5. O total de bytes retornados.
  def getBytes(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("get bytes")

    logs.select(logs.col("Requisicao"),logs.col("Bytes").cast("int")).groupBy("Requisicao").sum("Bytes").show()
//      +--------------------+----------+
//      |          Requisicao|sum(Bytes)|
//      +--------------------+----------+
//      |GET /cgi-bin/imag...|      1776|
//      |GET /shuttle/miss...|     21075|
//      |GET /cgi-bin/imag...|       952|
//      |       GET /ksc.html|   1609379|
//      |GET /shuttle/miss...|   5945554|
//      +--------------------+----------+

    logs.createOrReplaceTempView("erros")
    logs.sqlContext.sql("select Requisicao, SUM(CAST(Bytes as int)) as total_bytes from erros group by Requisicao").show()

    //      +--------------------+-----------+
//      |          Requisicao|total_bytes|
//      +--------------------+-----------+
//      |GET /cgi-bin/imag...|       1776|
//      |GET /shuttle/miss...|      21075|
//      |GET /cgi-bin/imag...|        952|
//      +--------------------+-----------+

  }
}
