package com.github.abulychev.iris.gossip

import java.net.InetSocketAddress
import akka.actor.{ActorRef, ActorLogging, Actor}
import com.github.abulychev.iris.gossip.serializer.SimpleApplicationStateSerializer
import akka.io.IO
import com.github.abulychev.iris.gossip.node.ApplicationState

/**
 * User: abulychev
 * Date: 9/26/14
 */
class TestActor(generation: Int, localAddress: InetSocketAddress, seeds: List[InetSocketAddress]) extends Actor with ActorLogging {
  override def preStart() {
    import context.system
    implicit val serializer = new SimpleApplicationStateSerializer

    IO(Gossip) ! Gossip.Bind(self, localAddress, generation, seeds)
  }

  def receive = {
    case Gossip.Bound =>
      log.info("Bound!")
      context.become(ready(sender()))
      sender ! Gossip.UpdateState(ApplicationState("hello", "world!"))
  }

  def ready(socket: ActorRef): Receive = {
    case Gossip.StateUpdated(endpoint, state) =>
      log.info(s"$endpoint update his state: $state")

    case Gossip.Discovered(endpoint) =>
      log.info(s"Discovered $endpoint")

    case Gossip.Restarted(endpoint) =>
      log.info(s"Restarted $endpoint")

    case Gossip.Reachable(endpoint) =>
      log.info(s"$endpoint is reachable")

    case Gossip.Unreachable(endpoint) =>
      log.info(s"$endpoint is unreachable")


    case _ =>
  }
}
