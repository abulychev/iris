package com.github.abulychev.iris.model

import java.security.MessageDigest
import org.apache.commons.codec.binary.Hex

/**
 * User: abulychev
 * Date: 11/11/14
 */
case class Chunk(id: String, offset: Int, size: Int) {
  val range = offset to (offset + size - 1)
  val persisted = !this.isInstanceOf[NotPersisted]
}

trait NotPersisted

object NotPersistedChunk {
  def apply(id: String, offset: Int, size: Int) = {
    new Chunk(id, offset, size) with NotPersisted
  }
}

object ChunkUtils {
  def intersect(x: Range, y: Range) =
    Math.max(x.start, y.start) to Math.min(x.end, y.end)

  def overlap(x: Range, y: Range): Boolean =
    (x contains y.start) || (y contains x.start)

  def sha1(array: Array[Byte]): String = {
    val md = MessageDigest.getInstance("SHA-1")
    md.reset()
    md.update(array)
    new String(Hex.encodeHex(md.digest()))
  }

  def sha1(arrays: Array[Byte]*): String = {
    val md = MessageDigest.getInstance("SHA-1")
    md.reset()
    arrays foreach { array => md.update(array) }
    new String(Hex.encodeHex(md.digest()))
  }
}