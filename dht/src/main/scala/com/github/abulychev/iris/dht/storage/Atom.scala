package com.github.abulychev.iris.dht.storage

/**
 * User: abulychev
 * Date: 10/11/14
 */
trait Atom {
  def value: Array[Byte]

  def compare(that: Atom): Int = {
    val l = Math.min(this.value.length, that.value.length)
    var i = 0
    while (i < l && this.value(i) == that.value(i)) i += 1
    if (i == l) this.value.length - that.value.length else this.value(i) - that.value(i)
  }

  override def hashCode: Int = {
    (value foldLeft 0)(_ * 31 + _)
  }

  override def equals(o: Any): Boolean = {
    o != null &&
      o.isInstanceOf[Atom] &&
      this.compare(o.asInstanceOf[Atom]) == 0
  }
}
