package com.github.abulychev.iris.gossip.serialize

import java.io.{DataInputStream, DataOutputStream}
import scala.collection.mutable
import com.github.abulychev.iris.gossip.node.{VersionedValue, HeartbeatState, ApplicationState, EndpointState}
import com.github.abulychev.iris.serialize.Serializer

/**
 * User: abulychev
 * Date: 9/26/14
 */
class EndpointStateSerializer[K,V](implicit val hbStateSerializer: Serializer[HeartbeatState],
                                   implicit val appStateSerializer: Serializer[ApplicationState[K,V]])
  extends LimitedSerializer[EndpointState[K,V]] {


  def writeTo(state: EndpointState[K, V], out: DataOutputStream, left: Int) {
    var l = left

    val versioned = (Option(state.heartbeatState).toList ++ state.applicationState.toList)
      .sortBy {
        case HeartbeatState(_, version) => version
        case (_, VersionedValue(_, version)) => version
      }

    versioned foreach {
      case hbState @ HeartbeatState(_, _) =>
        val bytes = hbStateSerializer.toBinary(hbState)

        if (l >= 1 + bytes.length) {
          l -= 1 + bytes.length
          out.writeByte(1)
          out.write(bytes, 0, bytes.length)
        } else {l=0}

      case (key: K, VersionedValue(value: V, version)) =>
        val bytes = appStateSerializer.toBinary(ApplicationState(key, value))

        if (l >= 1 + bytes.length + 4) {
          l -= 1 + bytes.length + 4
          out.writeByte(2)
          out.write(bytes, 0, bytes.length)
          out.writeInt(version)
        } else {l=0}
    }

    out.writeByte(0)
  }

  def readFrom(in: DataInputStream): EndpointState[K, V] = {
    var hbState: HeartbeatState = null
    val appStates = mutable.Map.empty[K, VersionedValue[V]]

    Iterator.continually({
      in.readByte() match {
        case 0 => None
        case 1 =>
          hbState = hbStateSerializer.readFrom(in)
          Some(Unit)

        case 2 =>
          val ApplicationState(key, value) = appStateSerializer.readFrom(in)
          val version = in.readInt()
          appStates += key -> VersionedValue(value, version)
          Some(Unit)
      }
    }).takeWhile(_.isDefined).foreach { case _ => }

    EndpointState(hbState, appStates.toMap)
  }
}
