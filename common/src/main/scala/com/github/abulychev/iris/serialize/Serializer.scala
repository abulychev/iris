package com.github.abulychev.iris.serialize

import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream}

/**
 * User: abulychev
 * Date: 9/26/14
 */
trait Serializer[T] {
  def writeTo(obj: T, out: DataOutputStream)
  def readFrom(in: DataInputStream): T

  def toBinary(obj: T): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    writeTo(obj, out)
    out.close()
    bytes.toByteArray
  }

  def fromBinary(bytes: Array[Byte]): T = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    val obj = readFrom(in)
    in.close()
    obj
  }
}
