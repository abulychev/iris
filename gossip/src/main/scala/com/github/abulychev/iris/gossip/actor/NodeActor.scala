package com.github.abulychev.iris.gossip.actor

import akka.actor._
import java.net.InetSocketAddress
import concurrent.duration._
import scala.util.Random
import scala.collection.mutable
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node._
import com.github.abulychev.iris.gossip.Gossip
import com.github.abulychev.iris.util.rpc.{Rpc, ClientBuilder}
import com.github.abulychev.iris.util.rpc.ClientBuilder._
import com.github.abulychev.iris.gossip.node.GossipAck2
import com.github.abulychev.iris.gossip.node.GossipAck
import com.github.abulychev.iris.gossip.node.ApplicationState
import scala.Some
import com.github.abulychev.iris.gossip.node.EndpointState
import com.github.abulychev.iris.gossip.node.GossipSyn
import akka.util.ByteString
import com.github.abulychev.iris.util.rpc.tcp.{ClientManager, Server}
import com.github.abulychev.iris.util.rpc.udp.UdpSocket

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
  with StatesHolder[K,V]
  with Serializing[K,V] {

  import NodeActor._

  require(!seeds.contains(localAddress))

  private val reachableEndpoints = mutable.Set.empty[InetSocketAddress]
  private val unreachableEndpoints = mutable.Set.empty[InetSocketAddress]

  private val failureDetector = context.actorOf(FailureDetector.props(self), "failure-detector")

  /* UDP */
  val socket = context.actorOf(Props(classOf[UdpSocket], localAddress, self), "socket")
  implicit val cb = ClientBuilder(socket)

  /* TCP */
//  val socket = context.actorOf(Props(classOf[Server], localAddress, self), "socket")
//  val clients = context.actorOf(Props(classOf[ClientManager]), "clients")
//  implicit val cb = ClientBuilder(clients)

  context.watch(socket)

  override def preStart() {
    init(generation)
  }

  context.system.scheduler.schedule(1 second, 1 second, self, Exchange)(context.dispatcher)
  handler ! Gossip.Bound

  def receive = {
    case Terminated(actor) if socket == actor =>
      context.stop(self)

    case Rpc.Request(bytes) =>
      val packet = serializer.fromBinary(bytes.toArray)
      process(packet)

    case Rpc.Response(bytes) =>
      val packet = serializer.fromBinary(bytes.toArray)
      process(packet)

    case Rpc.Timeout =>
      // It's ok

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
      val bytes = ByteString(serializer.toBinary(syn, 500))

      val alive = randomLiveEndpoint
      
      alive foreach { case endpoint =>
        client(endpoint) ! Rpc.Request(bytes, 5.seconds)
      }
      
      val isSeed = alive.exists(seeds.contains)

      val unreachableEndpointCount = unreachableEndpoints.size
      val liveEndpointCount = reachableEndpoints.size
      val prob = unreachableEndpointCount / (liveEndpointCount + 1)
      if (unreachableEndpointCount > 0 && Random.nextDouble() < prob) {
        randomUnreachableEndpoint foreach { case endpoint =>
          client(endpoint) ! Rpc.Request(bytes, 5.seconds)
        }
      }

      if (!isSeed || liveEndpointCount < seeds.size) {
        randomSeedEndpoint foreach { case endpoint =>
          client(endpoint) ! Rpc.Request(bytes, 5.seconds)
        }
      }

    case msg =>
      log.error(s"Unhandled message: {$msg}")
  }

  def process = PartialFunction[GossipMessage, Unit] {
    //case GossipPacket(_, Some(generation), _) if this.generation != generation =>
    //  log.debug("Received message not for my generation. Drop it")


    case GossipSyn(digests) =>
      log.debug("Received Syn")

      val ack = GossipAck(generateEpStateMap(digests), generateDigests(digests))
      val bytes = ByteString(serializer.toBinary(ack, 500))

      sender ! Rpc.Request(bytes, 5.seconds)


    case GossipAck(epStates: Map[InetSocketAddress, EndpointState[K,V]], digests) =>
      log.debug("Received Ack")

      mergeWith(epStates)

      val ack2 = GossipAck2(generateEpStateMap(digests))
      val bytes = ByteString(serializer.toBinary(ack2, 500))

      sender ! Rpc.Response(bytes)

    case GossipAck2(epStates: Map[InetSocketAddress, EndpointState[K,V]]) =>
      log.debug("Received Ack2")

      mergeWith(epStates)
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
