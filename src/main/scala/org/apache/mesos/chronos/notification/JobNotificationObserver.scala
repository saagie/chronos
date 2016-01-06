package org.apache.mesos.chronos.notification

import java.util.logging.Logger

import akka.actor.ActorRef
import com.google.inject.Inject
import org.apache.mesos.chronos.scheduler.jobs._
import org.joda.time.{DateTimeZone, DateTime}
import scala.reflect.runtime.universe._
class JobNotificationObserver @Inject()(val notificationClients: List[ActorRef] = List(),
                                      val clusterName: Option[String] = None) {
  private[this] val log = Logger.getLogger(getClass.getName)
  val clusterPrefix = clusterName.map(name => s"[$name]").getOrElse("")

  def asObserver: JobsObserver.Observer = JobsObserver.withName({
    case JobRemoved(job) => sendNotification(job, "%s [Chronos] Your job '%s' was deleted!".format(clusterPrefix, job.name), None, "removed")
    case JobDisabled(job, cause) => sendNotification(
      job,
      "%s [Chronos] job '%s' disabled".format(clusterPrefix, job.name),
      Some(cause), "disabled")

    case JobRetriesExhausted(job, taskStatus, attempts) =>
      val msg = "\n'%s'. Retries attempted: %d.\nTask id: %s\n"
        .format(DateTime.now(DateTimeZone.UTC), job.retries, taskStatus.getTaskId.getValue)
      sendNotification(job, "%s [Chronos] job '%s' failed!".format(clusterPrefix, job.name),
        Some(TaskUtils.appendSchedulerMessage(msg, taskStatus)), "retriesExhausted")

    case JobQueued(job, taskStatus, attempt) =>
      sendNotification(job, "%s [Chronos] job '%s' queued".format(clusterPrefix, job.name), None, "queued")

    case JobSkipped(job, date) =>
      sendNotification(job, "%s [Chronos] job '%s' queued".format(clusterPrefix, job.name), None, "skipped")

    case JobStarted(job, taskStatus, attempt) =>
      sendNotification(job, "%s [Chronos] job '%s' started".format(clusterPrefix, job.name), None, "started")

    case JobFinished(job, taskStatus, attempt) =>
      sendNotification(job, "%s [Chronos] job '%s' finished".format(clusterPrefix, job.name), None, "finished")

    case JobFailed(job, taskStatus, attempt) =>
      if (job.isRight) {
        val j = job.right.get
        sendNotification(j, "%s [Chronos] job '%s' failed".format(clusterPrefix, j.name), None, "failed")
      }

    case JobExpired(job, taskId) =>
      sendNotification(job, "%s [Chronos] job '%s' expired".format(clusterPrefix, job.name), None, "expired")

  }, getClass.getSimpleName)

  def sendNotification(job: BaseJob, subject: String, message: Option[String] = None, status: String) {
    for (client <- notificationClients) {
      val subowners = job.owner.split("\\s*,\\s*")
      for (subowner <- subowners) {
        log.info("Sending mail notification to:%s for job %s using client: %s".format(subowner, job.name, client))
        client !(job, subowner, subject, message, status)
      }
    }

    log.info(subject)
  }

}