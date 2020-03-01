package br.semantix.desafios

class Quantidade404Dia {

  def getHosts(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("hosts")

    logs.createOrReplaceTempView("hosts")

    println(logs.select("Host").distinct().count())
    logs.sqlContext.sql("select count(distinct Host) from hosts").show()
    //    +--------------------+
    //    |count(DISTINCT Host)|
    //    +--------------------+
    //    |              137979|
    //    +--------------------+
  }
}
