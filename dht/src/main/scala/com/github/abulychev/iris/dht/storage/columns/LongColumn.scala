package com.github.abulychev.iris.dht.storage.columns

/**
 * User: abulychev
 * Date: 10/20/14
 */

import com.github.abulychev.iris.dht.storage.Column
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer

/**
 * User: abulychev
 * Date: 10/20/14
 */
class LongColumn(val long: Long) extends Column(LongColumn.serializer.toBinary(long)) {

}

object LongColumn {
  private val serializer = new Serializer[Long] {
    def writeTo(value: Long, out: DataOutputStream) = {
      out.writeLong(value)
    }

    def readFrom(in: DataInputStream): Long = {
      in.readLong()
    }
  }

  def apply(value: Long) = new LongColumn(value)

  def unapply(column: Column): Option[Long] = {
    Some(serializer.fromBinary(column.value))
  }
}
