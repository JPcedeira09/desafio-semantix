package br.semantix.desafios

class Quantidade404Dia {

  def getErrosDia(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("erros por dia")

    logs.createOrReplaceTempView("erros")
    logs.sqlContext.sql("select TO_DATE(data_timestamp, 'dd/MMM/yyyy:hh:mm:ss Z') as data,count(Codigo) from erros where Codigo = '404' group by TO_DATE(data_timestamp, 'dd/MMM/yyyy:hh:mm:ss Z')").show()

    //    +----------+-------------+
    //    |      data|count(Codigo)|
    //    +----------+-------------+
    //    |1995-07-27|          154|
    //    |1995-07-13|          279|
    //    |1995-08-24|          141|
    //    |1995-07-07|          228|
    //    |1995-07-12|          206|
    //    +----------+-------------+

  }
}
