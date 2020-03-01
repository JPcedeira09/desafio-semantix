package br.semantix.desafios

class Total404 {

  def getErros(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("hosts")


//    println(logs.select("Host").distinct().count())

    logs.createOrReplaceTempView("erros")
    logs.sqlContext.sql("select count(Codigo) from erros where Codigo = '404'").show()
//    +-------------+
//    |count(Codigo)|
//    +-------------+
//    |        20868|
//    +-------------+
  }
}
