package nl.tudelft.ewi.se.in4355.client

import grizzled.slf4j.Logger
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.util.EntityUtils

object GridClientApp {

  private val LOG = Logger("nl.tudelft.ewi.se.in4355.ClientApp")

  private val HOSTNAME = "localhost"
  private val PORT = 10001

  def main(args: Array[String]) {

    LOG.info("Starting client - host: " + HOSTNAME + ":" + PORT)

    val client = new GridClient(HOSTNAME, PORT)
    val executor = new WordCountExecutor(client)
    executor.go

  }

}
