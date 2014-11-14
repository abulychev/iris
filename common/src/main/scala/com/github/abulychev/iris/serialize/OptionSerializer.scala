package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
case class OptionSerializer[T](serializer: Serializer[T]) extends Serializer[Option[T]] {
  def writeTo(opt: Option[T], out: DataOutputStream) {
    opt match {
      case Some(v) =>
        out.writeByte(1)
        serializer.writeTo(v, out)
      case None =>
        out.writeByte(0)
    }
  }

  def readFrom(in: DataInputStream): Option[T] = {
    in.readByte() match {
      case 0 => None
      case _ => Some(serializer.readFrom(in))
    }
  }
}
