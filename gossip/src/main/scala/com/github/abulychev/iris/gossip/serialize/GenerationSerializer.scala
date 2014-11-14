package com.github.abulychev.iris.gossip.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation

/**
 * User: abulychev
 * Date: 10/3/14
 */
class GenerationSerializer extends Serializer[Generation] {
  def writeTo(generation: Generation, out: DataOutputStream) {
    out.writeInt(generation)
  }

  def readFrom(in: DataInputStream): Generation = {
    in.readInt()
  }
}
