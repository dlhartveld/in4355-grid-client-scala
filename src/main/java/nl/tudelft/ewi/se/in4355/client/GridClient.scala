package nl.tudelft.ewi.se.in4355.client

import grizzled.slf4j.Logger
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.HttpResponse
import org.apache.http.util.EntityUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.StringEntity
import org.apache.http.HttpStatus

class GridClient(val hostname: String, val port: Int) {

  private val LOG = Logger(classOf[GridClient])

  def url = "http://" + hostname + ':' + port + "/"

  private val client = new DefaultHttpClient

  def post(path: String): HttpResponse = {
    LOG.trace("POST: " + path)
    client.execute(httpPostForUri(url + sanitized(path)))
  }

  def post(path: String, contentType: String, contents: String): HttpResponse = {
    LOG.trace("POST: " + path + " - content-type: " + contentType + " - contents: " + contents)
    client.execute(httpPostForUri(url + sanitized(path), contentType, contents))
  }

  private def contentsOfResponse(response: HttpResponse) = response.getEntity() match {
    case null => ""
    case _ => EntityUtils.toString(response.getEntity())
  }

  private def httpGetForUri(uri: String): HttpGet = {
    LOG.trace("Creating GET for URI: " + uri)
    new HttpGet(uri)
  }

  private def httpPostForUri(uri: String): HttpPost = {
    LOG.trace("Creating POST for URI: " + uri)
    new HttpPost(uri)
  }

  private def httpPostForUri(uri: String, contentType: String, contents: String): HttpPost = {
    LOG.trace("Creating POST for URI: " + uri + " - content-type: " + contentType + " - contents: " + contents)
    val post = new HttpPost(uri)
    val entity = new StringEntity(contents)
    entity.setContentType(contentType)
    post.setEntity(entity)
    post
  }

  private def sanitized(path: String): String = path match {
    case path if path startsWith "/" => path.tail
    case _ => path
  }

}