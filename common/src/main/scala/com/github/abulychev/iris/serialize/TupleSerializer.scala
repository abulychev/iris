package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
case class TupleSerializer[K,V](serializer1: Serializer[K],
                                serializer2: Serializer[V]) extends Serializer[(K,V)] {

  def writeTo(tuple: (K,V), out: DataOutputStream) {
    serializer1.writeTo(tuple._1, out)
    serializer2.writeTo(tuple._2, out)
  }

  def readFrom(in: DataInputStream): (K,V) = {
    (serializer1.readFrom(in), serializer2.readFrom(in))
  }
}
