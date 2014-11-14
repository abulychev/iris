package com.github.abulychev.iris.gossip.actor

import akka.actor.{ActorLogging, ActorRef, Actor}
import java.net.InetSocketAddress
import concurrent.duration._
import scala.util.Random
import scala.collection.mutable
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node._
import com.github.abulychev.iris.gossip.node.ApplicationState
import scala.Some
import com.github.abulychev.iris.gossip.node.GossipPacket
import com.github.abulychev.iris.gossip.node.GossipSyn
import com.github.abulychev.iris.gossip.Gossip

/**
 * User: abulychev
 * Date: 9/25/14
 */
class NodeActor[K,V](handler: ActorRef,
                     val localAddress: InetSocketAddress,
                     generation: Generation,
                     val seeds: List[InetSocketAddress])
                    (implicit val appStateSerializer: Serializer[ApplicationState[K,V]])
  extends Actor
  with ActorLogging
  with StatesHolder[K,V] {

  import NodeActor._

  require(!seeds.contains(localAddress))

  private val reachableEndpoints = mutable.Set.empty[InetSocketAddress]
  private val unreachableEndpoints = mutable.Set.empty[InetSocketAddress]

  private val connector = context.actorOf(UdpConnector.props(self, localAddress, generation), "udp-connector")
  private val failureDetector = context.actorOf(FailureDetector.props(self), "failure-detector")

  override def preStart() {
    init(generation)
  }

  def receive = {
    case Gossip.Bound =>
      import scala.concurrent.ExecutionContext.Implicits.global
      context.become(ready(sender()))
      context.system.scheduler.schedule(1 second, 1 second, self, Exchange)
      handler ! Gossip.Bound
  }

  def ready(socket: ActorRef): Receive = {
    case UdpConnector.Received(packet, remote) =>
      packet match {
        case GossipPacket(_, Some(generation), _) if this.generation != generation =>
          log.debug("Received message not for my generation. Drop it")


        case GossipPacket(Some(rGeneration), _, GossipSyn(digests)) =>
          log.debug("Received Syn")

          val ack = GossipAck(generateEpStateMap(digests), generateDigests(digests))
          val msg = GossipPacket(Some(generation), Some(rGeneration), ack)

          socket ! UdpConnector.Send(msg, remote)

        case GossipPacket(Some(rGeneration), _, GossipAck(epStates: Map[InetSocketAddress, EndpointState[K,V]], digests)) =>
          log.debug("Received Ack")

          mergeWith(epStates)

          val ack2 = GossipAck2(generateEpStateMap(digests))
          val msg = GossipPacket(None, Some(rGeneration), ack2)

          socket ! UdpConnector.Send(msg, remote)

        case GossipPacket(_, _, GossipAck2(epStates: Map[InetSocketAddress, EndpointState[K,V]])) =>
          log.debug("Received Ack2")

          mergeWith(epStates)

      }

    case FailureDetector.Reachable(endpoint, generation) =>
      if (generations(endpoint) == generation) {
        handler ! Gossip.Reachable(endpoint)
        reachableEndpoints += endpoint
      }

    case FailureDetector.Unreachable(endpoint, generation) =>
      if (generations(endpoint) == generation) {
        handler ! Gossip.Unreachable(endpoint)
        unreachableEndpoints += endpoint
      }


    case Gossip.UpdateState(state) =>
      setState(state.asInstanceOf[ApplicationState[K,V]])

    case Exchange =>
      incrementHeartbeat()

      val syn = GossipSyn(generateDigests)
      val msg = GossipPacket(Some(generation), None, syn)

      val alive = randomLiveEndpoint
      
      alive foreach { case endpoint =>
        socket ! UdpConnector.Send(msg, endpoint)
      }
      
      val isSeed = alive.exists(seeds.contains)

      val unreachableEndpointCount = unreachableEndpoints.size
      val liveEndpointCount = reachableEndpoints.size
      val prob = unreachableEndpointCount / (liveEndpointCount + 1)
      if (unreachableEndpointCount > 0 && Random.nextDouble() < prob) {
        randomUnreachableEndpoint foreach { case endpoint =>
          socket ! UdpConnector.Send(msg, endpoint)
        }
      }

      if (!isSeed || liveEndpointCount < seeds.size) {
        randomSeedEndpoint foreach { case endpoint =>
          socket ! UdpConnector.Send(msg, endpoint)
        }
      }

    case msg =>
      log.error(s"Unhandled message: {$msg}")
  }

  def mergeWith(epStates: Map[InetSocketAddress, EndpointState[K,V]]): Unit = mergeWith(
    (endpoint: InetSocketAddress, generation: Generation)  => {
      unreachableEndpoints += endpoint
      handler ! Gossip.Discovered(endpoint)
      failureDetector ! FailureDetector.Discovered(endpoint, generation)
    },
    (endpoint: InetSocketAddress, generation: Generation) => {
      handler ! Gossip.Restarted(endpoint)
      failureDetector ! FailureDetector.Restarted(endpoint, generation)
    },
    (endpoint: InetSocketAddress) => {
      failureDetector ! FailureDetector.Heartbeat(endpoint)
    },
    (endpoint: InetSocketAddress, state: ApplicationState[K,V]) => {
      handler ! Gossip.StateUpdated(endpoint, state)
    }
  )(epStates)

  private def randomEndpoint[T](it: Iterable[T]): Option[T] = {
    val size = it.size
    if (size == 0) None else Some(it.iterator.drop(Random.nextInt(size)).next())
  }

  def randomLiveEndpoint: Option[InetSocketAddress] = randomEndpoint(reachableEndpoints)

  def randomUnreachableEndpoint: Option[InetSocketAddress] = randomEndpoint(unreachableEndpoints)
  
  def randomSeedEndpoint: Option[InetSocketAddress] = randomEndpoint(seeds)
}

object NodeActor {
  private case object Exchange
}
