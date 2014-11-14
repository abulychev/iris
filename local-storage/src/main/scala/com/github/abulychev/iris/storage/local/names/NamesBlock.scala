package com.github.abulychev.iris.storage.local.names

import com.github.abulychev.iris.model.PathInfo

/**
 * User: abulychev
 * Date: 10/21/14
 */
class NamesBlock private(val paths: Map[String, PathInfo]) {
  def merge(that: NamesBlock): NamesBlock = {
    NamesBlock((this.paths.values ++ that.paths.values).toList)
  }
}

object NamesBlock {
  def apply(paths: List[PathInfo]): NamesBlock = {
    new NamesBlock(
      paths
        .groupBy { _.path }
        .mapValues { _.maxBy(_.timestamp) }
    )
  }

  def apply(paths: PathInfo*): NamesBlock = apply(paths.toList)
}
