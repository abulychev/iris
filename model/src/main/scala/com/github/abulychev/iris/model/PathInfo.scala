package com.github.abulychev.iris.model

import java.net.{URLDecoder, URLEncoder}

/**
 * User: abulychev
 * Date: 3/5/14
 */

sealed trait PathInfo {
  def path: String
  def timestamp: Long

  val tokens = PathInfoUtils.getTokens(path)
}

case class FileInfo(path: String, id: String, size: Int, timestamp: Long) extends PathInfo
case class DirectoryInfo(path: String, timestamp: Long) extends PathInfo
case class RemovedInfo(path: String, timestamp: Long) extends PathInfo

object FileInfo {
  def computeId(content: FileContentInfo): String =
    ChunkUtils.sha1(content.chunks.map { _.id.getBytes } : _*)
}

object PathInfoUtils {
  def getTokens(path: String) = path.split('/').filterNot(_.isEmpty).toList

  def encode(path: String) = URLEncoder.encode(path, "UTF-8")
  def decode(path: String) = URLDecoder.decode(path, "UTF-8")
  def hash(path: String) = Integer.toHexString(path.hashCode) // TODO: modify this
}


