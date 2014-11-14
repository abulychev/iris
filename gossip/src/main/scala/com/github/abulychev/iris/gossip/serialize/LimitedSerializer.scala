package com.github.abulychev.iris.gossip.serialize

import java.io.{ByteArrayOutputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer

/**
 * User: abulychev
 * Date: 9/26/14
 */
trait LimitedSerializer[T] extends Serializer[T] {
  def writeTo(obj: T, out: DataOutputStream, left: Int)
  def writeTo(obj: T, out: DataOutputStream): Unit = writeTo(obj, out, LimitedSerializer.Unlimited)

  def toBinary(obj: T, left: Int): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    writeTo(obj, out, left)
    out.close()
    bytes.toByteArray
  }
}

object LimitedSerializer {
  val Unlimited = 1000000
}
