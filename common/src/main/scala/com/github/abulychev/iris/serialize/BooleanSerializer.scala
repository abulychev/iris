package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
object BooleanSerializer extends Serializer[Boolean] {
  def writeTo(value: Boolean, out: DataOutputStream) {
    out.writeBoolean(value)
  }

  def readFrom(in: DataInputStream): Boolean = {
    in.readBoolean()
  }
}