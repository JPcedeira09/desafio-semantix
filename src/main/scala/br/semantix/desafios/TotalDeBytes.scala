package br.semantix.desafios

class TotalDeBytes {

  def getBytes(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("hosts")

//    df.agg(sum("steps").cast("long")).first.getLong(0)
    println(logs.select(logs.col("Requisicao"),logs.col("Bytes").cast("int")).groupBy("Requisicao").sum("Bytes"))

//    logs.createOrReplaceTempView("erros")
//    logs.sqlContext.sql("select count(Codigo) from erros where Codigo = '404'").show()

  }
}
