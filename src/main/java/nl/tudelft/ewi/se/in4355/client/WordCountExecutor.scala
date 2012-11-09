package nl.tudelft.ewi.se.in4355.client

import org.apache.http.HttpResponse
import scala.util.parsing.json.JSON
import org.apache.http.util.EntityUtils
import grizzled.slf4j.Logger
import scala.collection.immutable.HashMap
import scala.collection.mutable.StringBuilder
import org.apache.http.conn.HttpHostConnectException
import org.apache.http.HttpStatus

class WordCountExecutor(client: GridClient) {

  private val LOG = Logger(classOf[WordCountExecutor])

  private val stopwords = List("a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "as", "at", "be", "because", "been", "but", "by", "can", "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him", "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so", "some", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your")

  private val mapCode = readResource("wordcount-mappercombiner.js")
  private val reduceCode = readResource("wordcount-reducer.js")

  def go {

    var job = client.obtainNextJobId
    while (true) {
      LOG.info("\n\nStarting job: " + job + "\n")

      try {
        val jsCode = client.getCodeForJob(job)
        if (jsCode == mapCode) {
          doMap(job)
        } else if (jsCode == reduceCode) {
          doReduce(job)
        } else {
          LOG.warn("Unknown code:\n" + jsCode)
        }

        LOG.info("\n\nJob done: " + job + "\n")
      } catch {
        case e: Exception => LOG.error("Exception was thrown. Restarting...", e)
      }

      job = client.obtainNextJobId
    }

    LOG.info("No more jobs to be done.")

  }

  private def doMap(job: Int) {
    LOG.info("Executing MAP job: " + job)

    var hasMoreData = true
    while (hasMoreData) {
      val data = client.nextDataForJob(job)
      val task = data._1
      LOG.info("Got task id: " + task)

      val list = parse(data._2).getOrElse("value", List()).asInstanceOf[List[String]]

      val textBuilder = new StringBuilder
      list.foreach(w => textBuilder.append(w).append(' '))
      val text = textBuilder.deleteCharAt(textBuilder.size - 1).toString

      val words = text.split(" ").map(s => s.toLowerCase.replaceAll("[^0-9A-Za-z]", "")).filter(s => s.nonEmpty && !stopwords.contains(s))

      val resultBuilder = new StringBuilder().append("{\"id\":").append(task).append(",\"wordCounts\":[")
      words.to.foreach(w => resultBuilder.append("{\"word\":\"").append(w).append("\",\"count\":1},"))
      words.foreach(w => resultBuilder.append("{\"word\":\"" + w + "\",\"count\":1},"))
      resultBuilder.deleteCharAt(resultBuilder.size - 1).append("]}")

      val resultData = resultBuilder.toString

      hasMoreData = client.sendResultSetFor(job, resultData)
      LOG.info("Has more data? " + hasMoreData)
    }
  }

  private def doReduce(job: Int) {
    LOG.info("Executing REDUCE job: " + job)

    var hasMoreData = true
    while (hasMoreData) {
      val data = client.nextDataForJob(job)
      val task = data._1
      LOG.info("Got task id: " + task + " - computing result...")

      val resultMap = new scala.collection.mutable.HashMap[String, Int]

      val counts = parse(data._2).getOrElse("value", List()).asInstanceOf[List[Map[String, Any]]]
      counts.foreach((wordcount) => {
        val word = wordcount.getOrElse("word", "").asInstanceOf[String]
        val count = wordcount.getOrElse("count", -1.0).asInstanceOf[Double].toInt
        if (count == -1) {
          return
        } else {
          resultMap.put(word, resultMap.getOrElse(word, 0) + 1)
        }
      })

      val sb = new StringBuilder
      sb.append("{\"id\":").append(task).append(",\"value\":[")
      resultMap.foreach(wordcount => {
        sb.append("{\"word\":\"")
        sb.append(wordcount._1)
        sb.append("\",\"count\":")
        sb.append(wordcount._2)
        sb.append("},")
      })
      sb.deleteCharAt(sb.size - 1).append("]}")
      val resultData = sb.toString

      LOG.info("POSTing result...")
      hasMoreData = client.sendResultSetFor(job, resultData)
      LOG.info("Has more data? " + hasMoreData)
    }
  }

  private def parse(json: String): Map[String, Any] = {
    JSON.parseFull(json).getOrElse(new HashMap[String, Any]).asInstanceOf[Map[String, Any]]
  }

  private def resourceStream(fileName: String): java.io.InputStream = {
    getClass.getResourceAsStream("/" + fileName)
  }

  private def readResource(fileName: String): String = {
    scala.io.Source.fromInputStream(resourceStream(fileName)).getLines().filter((s) => s.nonEmpty).reduce((x, y) => x + "\n" + y)
  }

}
