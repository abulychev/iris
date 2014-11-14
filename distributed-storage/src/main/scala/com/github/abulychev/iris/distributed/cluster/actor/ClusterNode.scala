package com.github.abulychev.iris.distributed.cluster.actor

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import java.net.InetSocketAddress
import akka.io.IO
import scala.collection.mutable
import com.github.abulychev.iris.distributed.cluster.serialize.ApplicationStateSerializer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.gossip.node.ApplicationState
import com.github.abulychev.iris.gossip.Gossip
import Gossip.{StateUpdated, UpdateState}
import com.github.abulychev.iris.dht.actor.Token
import com.github.abulychev.iris.gossip.Gossip

/**
 * User: abulychev
 * Date: 10/20/14
 */
class ClusterNode(endpoint: InetSocketAddress,
                  generation: Generation,
                  seeds: List[InetSocketAddress],
                  handler: ActorRef,
                  dhtPort: Int,
                  dataPort: Int,
                  token: Token) extends Actor with ActorLogging {

  import ClusterNode._

  val registred = mutable.Set.empty[InetSocketAddress]
  val dht = mutable.Map.empty[InetSocketAddress, InetSocketAddress]
  val data = mutable.Map.empty[InetSocketAddress, InetSocketAddress]
  val tokens = mutable.Map.empty[InetSocketAddress, Token]

  override def preStart() {
    import context.system
    IO(Gossip) ! Gossip.Bind(self, endpoint, generation, seeds)(ApplicationStateSerializer)
  }

  def receive = {
    case Gossip.Bound =>
      sender ! Gossip.UpdateState(ApplicationState("token", token))
      sender ! Gossip.UpdateState(ApplicationState("dht-port", dhtPort.toString))
      sender ! Gossip.UpdateState(ApplicationState("data-port", dataPort.toString))

      context.become(ready(sender()))
  }

  def ready(socket: ActorRef): Receive = {
    case UpdateVersion(version) =>
      socket ! UpdateState(ApplicationState("version", version.toString))

    case StateUpdated(endpoint, ApplicationState("token", remoteToken: Token)) =>
      tokens += endpoint -> remoteToken

    case StateUpdated(endpoint, ApplicationState("version", version: String)) =>
      require(tokens.contains(endpoint), "`token` state should be already received")
      handler ! VersionUpdated(tokens(endpoint), version.toLong)

    case StateUpdated(endpoint, ApplicationState("dht-port", port: String)) =>
      require(tokens.contains(endpoint), "`token` state should be already received")
      val e = new InetSocketAddress(endpoint.getAddress, port.toInt)
      dht += endpoint -> e
      if (registred.contains(endpoint)) handler ! ReachableDht(e, tokens(endpoint))

    case StateUpdated(endpoint, ApplicationState("data-port", port: String)) =>
      val e = new InetSocketAddress(endpoint.getAddress, port.toInt)
      data += endpoint -> e
      if (registred.contains(endpoint)) handler ! ReachableData(e)


    case Gossip.Discovered(ep) =>

    case Gossip.Reachable(endpoint) =>
      registred += endpoint
      dht.get(endpoint) foreach { handler ! ReachableDht(_, tokens(endpoint)) }
      data.get(endpoint) foreach { handler ! ReachableData(_) }

    case Gossip.Unreachable(endpoint) =>
      dht.get(endpoint) foreach { handler ! UnreachableDht(_, tokens(endpoint)) }
      data.get(endpoint) foreach { handler ! UnreachableData(_) }

    case Gossip.Restarted(ep) =>
      registred -= ep
      dht -= ep
      data -= ep
      tokens -= ep

    case msg => log.error(s"Unhandled message: {$msg}")
  }

}


object ClusterNode {
  def props(endpoint: InetSocketAddress,
            generation: Generation,
            seeds: List[InetSocketAddress],
            namenode: ActorRef,
            dhtPort: Int,
            namesServicePort: Int,
            token: Token) =
    Props(classOf[ClusterNode],
      endpoint,
      generation,
      seeds,
      namenode,
      dhtPort,
      namesServicePort,
      token)

  case class UpdateVersion(version: Long)
  case class VersionUpdated(token: Token, version: Long)

  case class ReachableDht(endpoint: InetSocketAddress, token: Token)
  case class UnreachableDht(endpoint: InetSocketAddress, token: Token)

  case class ReachableData(endpoint: InetSocketAddress)
  case class UnreachableData(endpoint: InetSocketAddress)
}
