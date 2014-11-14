package com.github.abulychev.iris.gossip

import akka.actor._
import akka.io.IO
import java.net.InetSocketAddress
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.ApplicationState
import com.github.abulychev.iris.gossip.actor.{GossipManager, NodeActor}
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation

object Gossip extends ExtensionId[GossipExt] with ExtensionIdProvider {
  def lookup() = Gossip
  def createExtension(system: ExtendedActorSystem): GossipExt = new GossipExt(system)

  /* List of messages */
  case class Bind[K,V](handler: ActorRef,
                       localAddress: InetSocketAddress,
                       generation: Generation,
                       seeds: List[InetSocketAddress])
                      (implicit serializer: Serializer[ApplicationState[K,V]]) {
    /* TODO: how can I move it from here? */
    def props = Props(
      classOf[NodeActor[K,V]],
      handler,
      localAddress,
      generation,
      seeds.filterNot(_ == localAddress),
      serializer
    )
  }

  case object Bound
  case class Discovered(endpoint: InetSocketAddress)
  case class Reachable(endpoint: InetSocketAddress)
  case class Unreachable(endpoint: InetSocketAddress)
  case class Restarted(endpoint: InetSocketAddress)

  case class UpdateState[K,V](state: ApplicationState[K,V])
  case class StateUpdated[K,V](endpoint: InetSocketAddress, state: ApplicationState[K,V])
}

class GossipExt(system: ExtendedActorSystem) extends IO.Extension {
  val manager: ActorRef = {
    system.systemActorOf(
      props = Props(classOf[GossipManager]).withDeploy(Deploy.local),
      name = "IO-GOSSIP")
  }
}