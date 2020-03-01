package br.semantix.desafios

class NumeroDeHosts {

  def getHosts(): Unit = {
    val utils = new Utils()
    val logs = utils.getLogs("hosts")

    println(logs.select("Host").distinct().count())

    logs.createOrReplaceTempView("hosts")
    logs.sqlContext.sql("select count(distinct Host) from hosts").show()
//    +--------------------+
//    |count(DISTINCT Host)|
//    +--------------------+
//    |              137979|
//    +--------------------+
  }

}
