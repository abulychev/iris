package com.github.abulychev.iris.gossip

import akka.actor.{Props, ActorSystem}
import java.net.InetSocketAddress

/**
 * User: abulychev
 * Date: 9/25/14
 */
object Test extends App {
  val system = ActorSystem()
  val actors = (0 to 9).map { case i =>
    val port = 12340 + i
    val seeds = List(
      new InetSocketAddress("127.0.0.1", 12345),
      new InetSocketAddress("127.0.0.1", 12346),
      new InetSocketAddress("127.0.0.1", 12347)
    )

    system.actorOf(Props(classOf[TestActor], 2, new InetSocketAddress("127.0.0.1", port), seeds))

    Thread.sleep(200)
  }.toList

}
