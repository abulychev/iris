package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
case class ListSerializer[T](serializer: Serializer[T]) extends Serializer[List[T]] {
  def writeTo(list: List[T], out: DataOutputStream) {
    IntSerializer.writeTo(list.length, out)
    list foreach { serializer.writeTo(_, out) }
  }

  def readFrom(in: DataInputStream): List[T] = {
    (1 to IntSerializer.readFrom(in))
      .map { _ => serializer.readFrom(in) }
      .toList
  }
}
