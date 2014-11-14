package com.github.abulychev.iris.application

import java.io.File
import java.net.{URI, InetSocketAddress, InetAddress}
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._


/**
 * User: abulychev
 * Date: 3/19/14
 */
object Main extends App {

  val config = ConfigFactory.load().getConfig("com.github.abulychev.iris")

  val mountPoint = config.getString("mount-point")
  val home = new File(config.getString("home"))
  val host = InetAddress.getByName(config.getString("host"))
  val gossipPort = config.getInt("gossip.port")
  val dhtPort=  config.getInt("dht.port")
  val storagePort=  config.getInt("storage.port")

  val seeds = config.getStringList("gossip.seeds")
    .toList
    .map { parse }

  ApplicationBuilder.build(
    mountPoint,
    home,
    host,
    gossipPort,
    dhtPort,
    storagePort,
    seeds
  )

  def parse(address: String): InetSocketAddress = {
    val uri = new URI("http://" + address)
    new InetSocketAddress(uri.getHost, uri.getPort)
  }
}
