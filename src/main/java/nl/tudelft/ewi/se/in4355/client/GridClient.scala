package nl.tudelft.ewi.se.in4355.client

import grizzled.slf4j.Logger
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.HttpResponse
import org.apache.http.util.EntityUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.StringEntity
import org.apache.http.HttpStatus
import scala.util.parsing.json.JSON
import org.apache.http.conn.HttpHostConnectException
import scala.collection.immutable.HashMap

class GridClient(val hostname: String, val port: Int) {

  private val LOG = Logger(classOf[GridClient])

  def url = "http://" + hostname + ':' + port + "/"

  private val client = new DefaultHttpClient

  def obtainNextJobId(): Int = {
    var job = nextJobId
    while (job == -1) {
      LOG.info("Waiting for next job...")
      Thread.sleep(5000)
      job = nextJobId
    }
    job
  }

  private def nextJobId: Int = {
    LOG.error("nextJobId")
    try {
      val response = post("/resources/jobs")
      LOG.error("response: " + response)
      response match {
        case None => -1
        case Some(r) => {
          LOG.error("Some(r): " + r)
          r.getStatusLine().getStatusCode() match {
            case HttpStatus.SC_OK => {
              LOG.error("Status: OK")
              val id = contentsOf(response).getOrElse("-1").toInt
              LOG.error("ID: " + id)
              id
            }
            case _ => {
              LOG.error("Status: !OK")
              EntityUtils.consumeQuietly(r.getEntity())
              -1
            }
          }
        }
      }
    } catch {
      case e: HttpHostConnectException => -1
    }
  }

  def getCodeForJob(job: Int): String = {
    val response = post("/resources/jobs/" + job + "/code")
    contentsOf(response).getOrElse("").trim
  }

  def nextDataForJob(job: Int): (Int, String) = {
    val response = post("/resources/jobs/" + job + "/input")
    val contents = contentsOf(response).getOrElse("{\"id\": -1, \"value\": []}")
    val jid = parse(contents).getOrElse("id", -1.0).asInstanceOf[Double].toInt
    (jid, contents)
  }

  private def sendEmptyResultSetFor(job: Int, task: Int) = {
    sendResultSetFor(job, emptyResultSetFor(task))
  }

  def sendResultSetFor(job: Int, result: String) = {
    val response = post("/resources/jobs/" + job + "/output", "application/json", result)
    val contents = contentsOf(response).getOrElse("{\"hasMoreData\": false}")
    parse(contents).getOrElse("hasMoreData", false).asInstanceOf[Boolean]
  }

  private def post(path: String): Option[HttpResponse] = {
    LOG.trace("POST: " + path)

    try {
      val response = client.execute(httpPostForUri(url + sanitized(path)))
      LOG.trace("Result: " + response.getStatusLine())
      Some(response)
    } catch {
      case e: Exception => {
        LOG.warn("Exception occurred while POSTing", e)
        None
      }
    }

  }

  private def post(path: String, contentType: String, contents: String): Option[HttpResponse] = {
    LOG.trace("POST: " + path + " - content-type: " + contentType)
    //LOG.trace("POST: contents: " + contents)
    try {
      val response = client.execute(httpPostForUri(url + sanitized(path), contentType, contents))
      LOG.trace("Result: " + response.getStatusLine())
      Some(response)
    } catch {
      case e: Exception => None
    }
  }

  private def contentsOf(response: Option[HttpResponse]): Option[String] = response match {
    case None => None
    case Some(response) => {
      response.getEntity() match {
        case null => None
        case entity => new Some(EntityUtils.toString(entity, "UTF-8"))
      }
    }

  }

  private def httpGetForUri(uri: String): HttpGet = {
    new HttpGet(uri)
  }

  private def httpPostForUri(uri: String): HttpPost = {
    new HttpPost(uri)
  }

  private def httpPostForUri(uri: String, contentType: String, contents: String): HttpPost = {
    LOG.trace("Creating POST for URI: " + uri + " - content-type: " + contentType)
    //LOG.trace("Creating POST with contents: " + contents)
    val post = httpPostForUri(uri)
    val entity = new StringEntity(contents)
    entity.setContentType(contentType)
    post.setEntity(entity)
    post
  }

  private def sanitized(path: String): String = path match {
    case path if path startsWith "/" => path.tail
    case _ => path
  }

  private def emptyResultSetFor(task: Int) = "{\"id\": " + task + ", \"value\": []}"

  private def parse(json: String): Map[String, Any] = {
    JSON.parseFull(json).getOrElse(new HashMap[String, Any]).asInstanceOf[Map[String, Any]]
  }

}
