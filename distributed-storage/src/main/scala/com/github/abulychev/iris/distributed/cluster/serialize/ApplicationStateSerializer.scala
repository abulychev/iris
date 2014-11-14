package com.github.abulychev.iris.distributed.cluster.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.{ByteArraySerializer, StringSerializer, Serializer}
import com.github.abulychev.iris.gossip.node.ApplicationState
import com.github.abulychev.iris.dht.actor.Token

/**
 * User: abulychev
 * Date: 10/20/14
 */
object ApplicationStateSerializer extends Serializer[ApplicationState[String, Any]] {
  def writeTo(state: ApplicationState[String, Any], out: DataOutputStream) = {
    StringSerializer.writeTo(state.key, out)

    state match {
      case ApplicationState("token", token: Token) =>
        ByteArraySerializer.writeTo(token.value, out)

      case ApplicationState(key: String, value: String) =>
        StringSerializer.writeTo(value, out)
    }
  }

  def readFrom(in: DataInputStream): ApplicationState[String, Any] = {
    val key = StringSerializer.readFrom(in)
    key match {
      case "token" => ApplicationState(key, Token(ByteArraySerializer.readFrom(in)))
      case _ => ApplicationState(key, StringSerializer.readFrom(in))
    }
  }
}
