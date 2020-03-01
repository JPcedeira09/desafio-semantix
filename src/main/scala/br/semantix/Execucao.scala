package br.semantix

import br.semantix.desafios.{NumeroDeHosts, Quantidade404Dia, Total404, TotalDeBytes, Urls404}

object Execucao {

  def main(args: Array[String]): Unit = {

//    1. Número de hosts únicos.
    new NumeroDeHosts().getHosts()
//    2. O total de erros 404.
    new Total404()getErros()
//    3. Os 5 URLs que mais causaram erro 404
    new TotalDeBytes().getBytes()
//    4. Quantidade de erros 404 por dia
    new Quantidade404Dia().getErrosDia()
//    5. O total de bytes retornados.
    new Urls404().getUrls404()

  }
}
