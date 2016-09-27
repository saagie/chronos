package org.apache.mesos.chronos.notification

import java.io.{BufferedWriter, DataOutputStream, OutputStreamWriter, StringWriter}
import java.net.{HttpURLConnection, URL}
import java.util.logging.Logger

import org.apache.commons.codec.binary.Base64
import org.apache.mesos.chronos.scheduler.jobs.BaseJob
import com.fasterxml.jackson.core.JsonFactory
import org.joda.time.DateTime

class HttpClient(val endpointUrl: String, 
                 val credentials: Option[String]) extends NotificationClient {

  private[this] val log = Logger.getLogger(getClass.getName)

  def sendNotification(job: BaseJob, to: String, subject: String, message: Option[String], status: String, taskId: Option[String]) {

    val jsonBuffer = new StringWriter
    val factory = new JsonFactory()
    val generator = factory.createGenerator(jsonBuffer)

    // Create the payload
    generator.writeStartObject()

    if (subject != null && subject.nonEmpty) {
      generator.writeStringField("subject", subject)
    }
    if (message.nonEmpty && message.get.nonEmpty) {
      generator.writeStringField("message", message.get)
    }
    if (to != null && to.nonEmpty) {
      generator.writeStringField("to", to)
    }

    generator.writeStringField("date", DateTime.now.toString)
    generator.writeStringField("taskId", taskId.getOrElse(""))
    generator.writeStringField("status", status)
    generator.writeStringField("job", job.name.toString())
    generator.writeStringField("command", job.command.toString())
    generator.writeStringField("cpus", job.cpus.toString())
    generator.writeStringField("async", job.async.toString())
    generator.writeStringField("softError", job.softError.toString())
    generator.writeStringField("epsilon", job.epsilon.toString())
    generator.writeStringField("errorCount", job.errorCount.toString())
    generator.writeStringField("errorsSinceLastSuccess", job.errorsSinceLastSuccess.toString())
    generator.writeStringField("executor", job.executor.toString())
    generator.writeStringField("executorFlags", job.executorFlags.toString())
    generator.writeStringField("lastError", job.lastError.toString())
    generator.writeStringField("lastSuccess", job.lastSuccess.toString())
    generator.writeStringField("mem", job.mem.toString())
    generator.writeStringField("retries", job.retries.toString())
    generator.writeStringField("successCount", job.successCount.toString())
    generator.writeStringField("uris", job.uris.mkString(","))
    generator.writeStringField("lastHost", job.lastHost)

    generator.writeFieldName("lastPorts")
    generator.writeStartArray()
    job.currentPorts.foreach { p =>
      generator.writeNumber(p)
    }
    generator.writeEndArray()


    generator.writeFieldName("labels")
    generator.writeStartArray()
    job.labels.foreach { m =>
      generator.writeStartArray()
      generator.writeString(m._1)
      generator.writeString(m._2)
      generator.writeEndArray()
    }
    generator.writeEndArray()

    generator.writeEndObject()
    generator.flush()

    val payload = jsonBuffer.toString
    val auth = if(credentials.nonEmpty && credentials.get.nonEmpty) {
       "Basic " + new String(Base64.encodeBase64(credentials.get.getBytes()));
    } else {
      ""
    }


    var connection: HttpURLConnection = null
    try {
      val url = new URL(endpointUrl)
      connection = url.openConnection.asInstanceOf[HttpURLConnection]
      connection.setDoInput(true)
      connection.setDoOutput(true)
      connection.setUseCaches(false)
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type","application/json");

      if (auth.nonEmpty) {
        connection.setRequestProperty ("Authorization", auth);
      }

      val outputStream = new DataOutputStream(connection.getOutputStream)
      val writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))
      writer.write(payload)
      writer.close()
      outputStream.close()

      log.info("Sent message to http endpoint. Response code:" +
        connection.getResponseCode +
        " - " +
        connection.getResponseMessage)
    } finally {
      if (connection != null) {
        connection.disconnect()
      }
    }
  }
}
