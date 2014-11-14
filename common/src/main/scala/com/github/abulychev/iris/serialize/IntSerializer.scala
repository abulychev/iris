package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
object IntSerializer extends Serializer[Int] {
  def writeTo(value: Int, out: DataOutputStream) {
    out.writeInt(value)
  }

  def readFrom(in: DataInputStream): Int = {
    in.readInt()
  }
}
