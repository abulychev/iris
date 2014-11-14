package com.github.abulychev.iris.settings

/**
 * User: abulychev
 * Date: 10/31/14
 */
import akka.actor._
import scala.concurrent.duration._
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit

class SettingsImpl(config: Config) extends Extension {
  object Gossip {
    val GossipInterval = config.getDuration("gossip.gossip-interval", TimeUnit.MILLISECONDS).millisecond
    val PhiThreshold = config.getDouble("gossip.phi-threshold")
    val FailureDetectingInterval = config.getDuration("gossip.failure-detecting-interval", TimeUnit.SECONDS).seconds
  }

  object Dht {
    val ConnectingTimeout = config.getDuration("dht.connecting-timeout", TimeUnit.MILLISECONDS).millisecond
    val RequestTimeout = config.getDuration("dht.request-timeout", TimeUnit.MILLISECONDS).millisecond
  }

  object Storage {
    val ConnectingTimeout = config.getDuration("storage.connecting-timeout", TimeUnit.MILLISECONDS).millisecond
    val RequestTimeout = config.getDuration("storage.request-timeout", TimeUnit.MILLISECONDS).millisecond
  }
}

object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {
  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) =
    new SettingsImpl(system.settings.config.getConfig("com.github.abulychev.iris"))
}

trait IrisSettings { self: Actor =>
  val settings = Settings(context.system)
}
