package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
object StringSerializer extends Serializer[String] {
  def writeTo(value: String, out: DataOutputStream) {
    out.writeUTF(value)
  }

  def readFrom(in: DataInputStream): String = {
    in.readUTF()
  }
}
