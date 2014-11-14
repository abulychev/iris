package com.github.abulychev.iris.settings

import spray.can.Http.HostConnectorSetup
import akka.actor.ActorSystem
import scala.concurrent.duration.Duration
import spray.can.client.HostConnectorSettings

/**
 * User: abulychev
 * Date: 11/1/14
 */
case class HostConnectorSetupBuilder(connectingTimeout: Duration, requestTimeout: Duration) {
  def build(host: String, port: Int)(implicit system: ActorSystem) = {
    HostConnectorSetup(
      host = host,
      port = port,
      settings = {
        val settings = HostConnectorSettings(system)
        val connections = settings.connectionSettings.copy(
          connectingTimeout = connectingTimeout,
          requestTimeout = requestTimeout
        )
        Some(settings.copy(
          connectionSettings = connections,
          maxRetries = 0
        ))
      }
    )
  }
}
