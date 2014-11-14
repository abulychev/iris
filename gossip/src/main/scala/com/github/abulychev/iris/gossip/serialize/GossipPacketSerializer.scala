package com.github.abulychev.iris.gossip.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.gossip.node.{GossipPacket, GossipAck2, GossipAck, GossipSyn}

/**
 * User: abulychev
 * Date: 9/26/14
 */
class GossipPacketSerializer[K,V](implicit val generationSerializer: Serializer[Generation],
                                  implicit val synSerializer: LimitedSerializer[GossipSyn],
                                  implicit val ackSerializer: LimitedSerializer[GossipAck[K,V]],
                                  implicit val ack2Serializer: LimitedSerializer[GossipAck2[K,V]])
  extends LimitedSerializer[GossipPacket] {

  def writeTo(packet: GossipPacket, out: DataOutputStream, left: Int) {
    packet match {
      case GossipPacket(Some(sGeneration), _, syn @ GossipSyn(_)) =>
        out.writeByte(Opcode.Syn)
        
        generationSerializer.writeTo(sGeneration, out)
        synSerializer.writeTo(syn, out, left - 1)

      case GossipPacket(Some(sGeneration), Some(dGeneration), ack @ GossipAck(_, _)) =>
        out.writeByte(Opcode.Ack)
        
        generationSerializer.writeTo(sGeneration, out)
        generationSerializer.writeTo(dGeneration, out)        
        ackSerializer.writeTo(ack.asInstanceOf[GossipAck[K,V]], out, left - 1)

      case GossipPacket(_, Some(dGeneration), ack2 @ GossipAck2(_)) =>
        out.writeByte(Opcode.Ack2)

        generationSerializer.writeTo(dGeneration, out)
        ack2Serializer.writeTo(ack2.asInstanceOf[GossipAck2[K,V]], out, left - 1)
    }

  }

  def readFrom(in: DataInputStream): GossipPacket = {
    in.readByte() match {
      case Opcode.Syn =>
        val sGeneration = generationSerializer.readFrom(in)
        val syn = synSerializer.readFrom(in)
        GossipPacket(Some(sGeneration), None, syn)
        
      case Opcode.Ack =>
        val sGeneration = generationSerializer.readFrom(in)
        val dGeneration = generationSerializer.readFrom(in)
        val ack = ackSerializer.readFrom(in)
        GossipPacket(Some(sGeneration), Some(dGeneration), ack)
        
      case Opcode.Ack2 =>
        val dGeneration = generationSerializer.readFrom(in)
        val ack2 = ack2Serializer.readFrom(in)
        GossipPacket(None, Some(dGeneration), ack2)
    }
  }
}

object Opcode {
  val Syn = 1
  val Ack = 2
  val Ack2 = 3
}
