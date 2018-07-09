package org.apache.mesos.chronos.scheduler.config

object Features {

  //enable GPUs
  lazy val GPU_RESOURCES = "gpu_resources"

  lazy val availableFeatures = Map(
    GPU_RESOURCES -> "Enable support for GPU in Chronos (experimental)"
  )

  def description: String = {
    availableFeatures.map { case (name, description) => s"$name - $description" }.mkString(", ")
  }
}