package com.github.abulychev.iris.gossip.node

import com.github.abulychev.iris.serialize.{Serializer, EndpointSerializer}
import com.github.abulychev.iris.gossip.serialize._

/**
 * User: abulychev
 * Date: 10/3/14
 */
trait Serializing[K,V] {
  implicit def appStateSerializer: Serializer[ApplicationState[K,V]]

  implicit val endpointSerializer = EndpointSerializer
  implicit val generationSerializer = new GenerationSerializer
  implicit val digestSerializer = new GossipDigestSerializer
  implicit val synSerializer = new GossipSynSerializer
  implicit val hbSerializer = new HeartbeatStateSerializer
  implicit val eStateSerializer = new EndpointStateSerializer[K,V]
  implicit val eStateMapSerializer = new EndpointStatesMapSerializer[K,V]
  implicit val ackSerializer = new GossipAckSerializer[K,V]
  implicit val ack2Serializer = new GossipAck2Serializer[K,V]

  implicit val serializer = new GossipPacketSerializer[K,V]
}
