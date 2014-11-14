package com.github.abulychev.iris.dht.serialize

import com.github.abulychev.iris.serialize.{ByteArraySerializer, Serializer}
import com.github.abulychev.iris.dht.actor.Token
import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/29/14
 */
object TokenSerializer extends Serializer[Token] {
  def writeTo(token: Token, out: DataOutputStream) {
    ByteArraySerializer.writeTo(token.value, out)
  }

  def readFrom(in: DataInputStream): Token = {
    Token(ByteArraySerializer.readFrom(in))
  }
}
