package com.github.abulychev.iris.serialize

import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 10/23/14
 */
case class MapSerializer[K,V](keySerializer: Serializer[K],
                              valueSerializer: Serializer[V]) extends Serializer[Map[K,V]] {

  val serializer = ListSerializer(TupleSerializer(keySerializer, valueSerializer))

  def writeTo(map: Map[K,V], out: DataOutputStream) {
    serializer.writeTo(map.toList, out)
  }

  def readFrom(in: DataInputStream): Map[K,V] = {
    serializer.readFrom(in).toMap
  }
}
