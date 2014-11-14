package com.github.abulychev.iris.gossip.serialize

import java.net.InetSocketAddress
import java.io.{DataInputStream, DataOutputStream}
import scala.collection.mutable
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.EndpointState

/**
 * User: abulychev
 * Date: 9/26/14
 */
class EndpointStatesMapSerializer[K,V](implicit val endpointSerializer: Serializer[InetSocketAddress],
                                       implicit val eStateSerializer: LimitedSerializer[EndpointState[K,V]])
  extends LimitedSerializer[Map[InetSocketAddress, EndpointState[K,V]]] {

  def writeTo(eStateMap: Map[InetSocketAddress, EndpointState[K,V]], out: DataOutputStream, left: Int) = {
    var l = left

    eStateMap foreach { case (endpoint, state) =>
      val bytes1 = endpointSerializer.toBinary(endpoint)

      if (l > bytes1.length + 10) {
        val bytes2 = eStateSerializer.toBinary(state, l - 1 - bytes1.length)

        l -= 1 + bytes1.length + bytes2.length

        out.writeByte(1)
        out.write(bytes1, 0, bytes1.length)
        out.write(bytes2, 0, bytes2.length)
      }
    }

    out.writeByte(0)
  }

  def readFrom(in: DataInputStream): Map[InetSocketAddress, EndpointState[K,V]] = {
    val eStateMap = mutable.Map.empty[InetSocketAddress, EndpointState[K,V]]

    while (in.readByte() > 0) {
      val endpoint = endpointSerializer.readFrom(in)
      val state = eStateSerializer.readFrom(in)
      eStateMap += endpoint -> state
    }

    eStateMap.toMap
  }


}
