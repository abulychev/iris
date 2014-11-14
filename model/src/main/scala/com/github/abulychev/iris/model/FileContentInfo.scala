package com.github.abulychev.iris.model

/**
 * User: abulychev
 * Date: 3/6/14
 */
case class FileContentInfo(size: Int, chunks: List[Chunk]) {
  import ChunkUtils._

  def findChunks(range: Range) = {
    chunks.filter { case Chunk(_, offset, size) => overlap(range, offset to (offset + size - 1)) }
  }

}

