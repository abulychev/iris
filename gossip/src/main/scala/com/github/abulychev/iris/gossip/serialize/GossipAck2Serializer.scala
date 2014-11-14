package com.github.abulychev.iris.gossip.serialize

import java.net.InetSocketAddress
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.gossip.node.{GossipAck2, EndpointState}

/**
  * User: abulychev
  * Date: 9/26/14
  */
class GossipAck2Serializer[K,V](implicit val eStateMapSerializer: LimitedSerializer[Map[InetSocketAddress, EndpointState[K,V]]])
  extends LimitedSerializer[GossipAck2[K,V]] {

  def writeTo(ack: GossipAck2[K,V], out: DataOutputStream, left: Int) {
    /* TODO: Sorting */
    val bytes = eStateMapSerializer.toBinary(ack.endpointStates, left)
    out.write(bytes, 0, bytes.length)
  }

  def readFrom(in: DataInputStream): GossipAck2[K,V] = {
    val eStateMap = eStateMapSerializer.readFrom(in)
    GossipAck2(eStateMap)
  }
}
