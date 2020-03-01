package br.semantix.desafios

class Total404 {

//  2. O total de erros 404.
  def getErros(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("erros 404")

    logs.createOrReplaceTempView("erros")
    logs.sqlContext.sql("select count(Codigo) from erros where Codigo = '404'").show()
//    +-------------+
//    |count(Codigo)|
//    +-------------+
//    |        20868|
//    +-------------+
  }
}
