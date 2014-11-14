package com.github.abulychev.iris.gossip

import akka.actor.{Props, ActorSystem}
import java.net.InetSocketAddress

/**
 * User: abulychev
 * Date: 9/26/14
 */
object Test3 extends App {
  val system = ActorSystem()
  system.actorOf(Props(classOf[TestActor], 1, new InetSocketAddress("127.0.0.1", 12347), List(new InetSocketAddress("127.0.0.1", 12345))))

}
