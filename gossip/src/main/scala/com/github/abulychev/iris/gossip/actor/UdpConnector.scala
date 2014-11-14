package com.github.abulychev.iris.gossip.actor

import akka.actor.{Props, ActorLogging, ActorRef, Actor}
import akka.io.{Udp, IO}
import java.net.InetSocketAddress
import akka.util.ByteString
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.{GossipPacket, Serializing, ApplicationState}
import com.github.abulychev.iris.gossip.Gossip

/**
 * User: abulychev
 * Date: 10/3/14
 */
class UdpConnector[K,V](handler: ActorRef,
                        localAddress: InetSocketAddress,
                        generation: Generation)
                        (implicit val appStateSerializer: Serializer[ApplicationState[K,V]])
  extends Actor
  with ActorLogging
  with Serializing[K,V] {

  import UdpConnector._

  override def preStart() {
    import context.system
    IO(Udp) ! Udp.Bind(self, localAddress)
  }

  def receive = {
    case Udp.Bound(_) =>
      context.become(ready(sender()))
      handler ! Gossip.Bound
  }

  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      serializer.fromBinary(data.toArray) match {
        case GossipPacket(_, Some(generation), _) if this.generation != generation =>
          log.debug("Received message not for my generation. Drop it")

        case packet @ GossipPacket(_, _, _) =>
          handler ! Received(packet, remote)
      }

    case Send(packet, endpoint) =>
      val bytes = serializer.toBinary(packet, UdpLimit)
      socket ! Udp.Send(ByteString(bytes), endpoint)

    case msg =>
      log.error(s"Unhandled message: {$msg}")
  }
}

object UdpConnector {
  def props[K,V](handler: ActorRef, localAddress: InetSocketAddress, generation: Generation)
                (implicit appStateSerializer: Serializer[ApplicationState[K,V]]) =
    Props(classOf[UdpConnector[K,V]], handler, localAddress, generation, appStateSerializer)

  case class Received(packet: GossipPacket, endpoint: InetSocketAddress)
  case class Send(packet: GossipPacket, endpoint: InetSocketAddress)

  val UdpLimit = 500
}

