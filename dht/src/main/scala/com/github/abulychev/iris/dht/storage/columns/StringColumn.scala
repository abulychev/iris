package com.github.abulychev.iris.dht.storage.columns

import com.github.abulychev.iris.dht.storage.Column

/**
 * User: abulychev
 * Date: 10/20/14
 */
class StringColumn(val text: String) extends Column(text.getBytes)

object StringColumn {
  def apply(text: String) = new StringColumn(text)

  def unapply(column: Column): Option[String] = {
    Some(new String(column.value))
  }
}