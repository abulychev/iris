package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
object LongSerializer extends Serializer[Long] {
  def writeTo(value: Long, out: DataOutputStream) {
    out.writeLong(value)
  }

  def readFrom(in: DataInputStream): Long = {
    in.readLong()
  }
}
