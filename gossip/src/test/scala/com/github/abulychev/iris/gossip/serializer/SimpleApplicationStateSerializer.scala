package com.github.abulychev.iris.gossip.serializer

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.ApplicationState

/**
 * User: abulychev
 * Date: 9/26/14
 */
class SimpleApplicationStateSerializer extends Serializer[ApplicationState[String, String]] {
  def writeTo(state: ApplicationState[String, String], out: DataOutputStream) {
    out.writeUTF(state.key)
    out.writeUTF(state.value)
  }

  def readFrom(in: DataInputStream): ApplicationState[String, String] = {
    ApplicationState(in.readUTF(), in.readUTF())
  }
}
