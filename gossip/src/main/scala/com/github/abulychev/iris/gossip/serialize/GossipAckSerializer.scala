package com.github.abulychev.iris.gossip.serialize

import java.net.InetSocketAddress
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.gossip.node.{GossipAck, GossipSyn, EndpointState}

/**
 * User: abulychev
 * Date: 9/26/14
 */
class GossipAckSerializer[K,V](implicit val eStateMapSerializer: LimitedSerializer[Map[InetSocketAddress, EndpointState[K,V]]],
                               implicit val gossipSynSerializer: LimitedSerializer[GossipSyn])
  extends LimitedSerializer[GossipAck[K,V]] {

  def writeTo(ack: GossipAck[K,V], out: DataOutputStream, left: Int) {
    /* TODO: Sorting */
    val bytes1 = eStateMapSerializer.toBinary(ack.endpointStates, left)
    val bytes2 = gossipSynSerializer.toBinary(GossipSyn(ack.digests), left - bytes1.length)

    out.write(bytes1, 0, bytes1.length)
    out.write(bytes2, 0, bytes2.length)
  }

  def readFrom(in: DataInputStream): GossipAck[K,V] = {
    val eStateMap = eStateMapSerializer.readFrom(in)
    val syn = gossipSynSerializer.readFrom(in)
    GossipAck(eStateMap, syn.digests)
  }
}
