package com.github.abulychev.iris.gossip.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.gossip.node.HeartbeatState

/**
 * User: abulychev
 * Date: 9/26/14
 */
class HeartbeatStateSerializer(implicit val generationSerializer: Serializer[Generation])
  extends Serializer[HeartbeatState] {

  def writeTo(state: HeartbeatState, out: DataOutputStream) = {
    generationSerializer.writeTo(state.generation, out)
    out.writeInt(state.version)
  }

  def readFrom(in: DataInputStream): HeartbeatState = {
    val generation = generationSerializer.readFrom(in)
    val version = in.readInt()
    HeartbeatState(generation, version)
  }
}
