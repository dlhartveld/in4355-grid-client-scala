package nl.tudelft.ewi.se.in4355.client

import grizzled.slf4j.Logger
import org.apache.http.HttpResponse
import org.apache.http.util.EntityUtils
import scala.util.parsing.json.JSON
import java.util.Collections.EmptyMap
import scala.collection.immutable.HashMap

class WordCountExecutor(client: GridClient) {

  private val LOG = Logger(classOf[WordCountExecutor])

  def go {

    LOG.info("Single job execution ...")

    val id = nextJobId
    LOG.info("Got ID: " + id)

    val data = nextJobData(id)
    LOG.info("Got data: " + data)

  }

  private def nextJobId: Int = {
    val response = client.post("/resources/jobs")
    contentsOf(response).getOrElse("-1").toInt
  }

  private def nextJobData(id: Int): String = {
    val response = client.post("/resources/jobs/" + id + "/input")
    contentsOf(response).getOrElse("{\"id\": 0, \"value\": []}")
  }

  private def sendEmptyResultSetFor(id: Int) {
    client.post("/resources/jobs/" + id + "/output", "application/json", emptyResultSetFor(id))
  }

  private def contentsOf(response: HttpResponse): Option[String] = response.getEntity() match {
    case null => None
    case entity => new Some(EntityUtils.toString(entity))
  }

  private def parse(json: String) = {
    JSON.parseFull(json).getOrElse(new HashMap[String, Any]).asInstanceOf[Map[String, Any]]
  }

  private def emptyResultSetFor(task: Int) = "{\"id\": " + task + ", \"value\": \"\"}"

}
