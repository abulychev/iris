package com.github.abulychev.iris.dht.storage

import java.util.concurrent.ConcurrentSkipListMap
import java.util.Comparator
import scala.collection.mutable.ListBuffer

/**
 * User: abulychev
 * Date: 10/7/14
 */
class Table {
  private val map = new ConcurrentSkipListMap[Row, Long](new Comparator[Row] {
    override def compare(o1: Row, o2: Row): Int = o1 compare o2
  })

  def put(row: Row, expireTime: Long) {
    map.put(row, expireTime)
  }

  def get(row: Row): List[Row] = {
    val time = System.currentTimeMillis
    val it = map.tailMap(row).entrySet().iterator
    var less = true
    val buffer = ListBuffer[Row]()

    while (it.hasNext && less) {
      val e = it.next()
      val r = e.getKey
      val expireTime = e.getValue

      less = (r.columns zip row.columns) forall { case (c1, c2) => c1.compare(c2) == 0 }
      if (less) buffer += r

      if (expireTime < time) it.remove()
    }

    buffer.toList
  }

  def removeObsolete() {
    val time = System.currentTimeMillis
    val it = map.entrySet().iterator

    while (it.hasNext) {
      val e = it.next()
      val expireTime = e.getValue
      if (expireTime < time) it.remove()
    }
  }
}

case class Column(value: Array[Byte]) extends Atom with Ordered[Column] {
  override def equals(o: Any) = o.isInstanceOf[Column] && this.compare(o.asInstanceOf[Column]) == 0
  def compare(that: Column): Int = this compare that.asInstanceOf[Atom]
  override def toString = "Column(" + new String(value) + ")"
}

object Column {
  def apply(value: String): Column = Column(value.getBytes)
}

case class Row(columns: Column*) extends Ordered[Row] {
  def compare(that: Row): Int = {
    this.columns.iterator.zipAll(that.columns.iterator, null, null).map {
      case (null, _) => -1
      case (_, null) => 1
      case (c1, c2) => c1 compare c2
    }.find { _ != 0 }.
      getOrElse(0)
  }
}

object Row {
  def apply(columns: List[Column]): Row = new Row(columns: _*)
}




