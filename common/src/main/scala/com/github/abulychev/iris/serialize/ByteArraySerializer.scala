package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
object ByteArraySerializer extends Serializer[Array[Byte]] {
  def writeTo(array: Array[Byte], out: DataOutputStream) {
    IntSerializer.writeTo(array.length, out)
    out.write(array)
  }

  def readFrom(in: DataInputStream): Array[Byte] = {
    val length = IntSerializer.readFrom(in)
    val array = new Array[Byte](length)
    in.readFully(array)
    array
  }
}
