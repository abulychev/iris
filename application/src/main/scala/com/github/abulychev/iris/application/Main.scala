package com.github.abulychev.iris.application

import java.io.File
import java.net.{URI, InetSocketAddress, InetAddress}
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import org.slf4j.LoggerFactory


/**
 * User: abulychev
 * Date: 3/19/14
 */
object Main extends App {
  val log = LoggerFactory.getLogger("Main")

  log.info("Starting application ...")

  val home = new File(args(0))
  val mountPoint = new File(args(1))

  log.error(s"Loading application with home: {${home.getAbsolutePath}} and mount point: {${mountPoint.getAbsolutePath}}")

  val configurationFile = new File(home, "application.conf")
  if (!configurationFile.exists()) {
    log.error(s"Could not find configuration file: {${configurationFile.getAbsolutePath}}")
  }

  log.info("Loading configuration ...")
  val config = ConfigFactory.parseFile(configurationFile).withFallback(ConfigFactory.load())
  log.info(config.toString)

  val irisConfig = config.getConfig("com.github.abulychev.iris")

  val host = InetAddress.getByName(irisConfig.getString("host"))
  val gossipPort = irisConfig.getInt("gossip.port")
  val dhtPort=  irisConfig.getInt("dht.port")
  val storagePort=  irisConfig.getInt("storage.port")

  val seeds = irisConfig.getStringList("gossip.seeds")
    .toList
    .map { parse }

  ApplicationBuilder.build(
    mountPoint.getAbsolutePath,
    home,
    host,
    gossipPort,
    dhtPort,
    storagePort,
    seeds
  )

  private def parse(address: String): InetSocketAddress = {
    val uri = new URI("http://" + address)
    new InetSocketAddress(uri.getHost, uri.getPort)
  }
}
