package com.github.abulychev.iris.gossip.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.gossip.node._

/**
 * User: abulychev
 * Date: 9/26/14
 */
class GossipMessageSerializer[K,V](implicit val generationSerializer: Serializer[Generation],
                                   implicit val synSerializer: LimitedSerializer[GossipSyn],
                                   implicit val ackSerializer: LimitedSerializer[GossipAck[K,V]],
                                   implicit val ack2Serializer: LimitedSerializer[GossipAck2[K,V]])
  extends LimitedSerializer[GossipMessage] {

  def writeTo(message: GossipMessage, out: DataOutputStream, left: Int) {
    message match {
      case syn: GossipSyn =>
        out.writeByte(Opcode.Syn)
        synSerializer.writeTo(syn, out, left - 1)

      case ack: GossipAck[K,V] =>
        out.writeByte(Opcode.Ack)
        ackSerializer.writeTo(ack, out, left - 1)

      case ack2: GossipAck2[K,V] =>
        out.writeByte(Opcode.Ack2)
        ack2Serializer.writeTo(ack2, out, left - 1)
    }

  }

  def readFrom(in: DataInputStream): GossipMessage = {
    in.readByte() match {
      case Opcode.Syn => synSerializer.readFrom(in)
      case Opcode.Ack => ackSerializer.readFrom(in)
      case Opcode.Ack2 => ack2Serializer.readFrom(in)
    }
  }
}

object Opcode {
  val Syn = 1
  val Ack = 2
  val Ack2 = 3
}
