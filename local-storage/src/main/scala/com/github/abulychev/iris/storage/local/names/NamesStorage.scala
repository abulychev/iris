package com.github.abulychev.iris.storage.local.names

import java.io._
import org.apache.commons.io.FileUtils
import com.github.abulychev.iris.storage.local.names.serializer.NamesBlockSerializer
import scala.util.Try
import com.github.abulychev.iris.model.{DirectoryInfo, PathInfo}

/**
 * User: abulychev
 * Date: 10/21/14
 */
class NamesStorage(home: File) {
  home.mkdirs()

  def init = NamesBlock(DirectoryInfo("/", System.currentTimeMillis))

  private var current = {
    val files = home.listFiles().toSet
    val block = files.toIterator
      .map { file => FileUtils.readFileToByteArray(file) }
      .map { bytes => new DataInputStream(new ByteArrayInputStream(bytes)) }
      .flatMap {
        is => Iterator.continually( Try { NamesBlockSerializer.readFrom(is) } )
          .takeWhile(b => b.isSuccess)
          .map { _.get }
      }
      .foldLeft(init)({ case (a,b) => a merge b })

    /* Replacing old current */
    val current = new File(home, "current")
    val newCurrent = new File(home, ".current")
    val os = new DataOutputStream(new FileOutputStream(newCurrent))
    NamesBlockSerializer.writeTo(block, os)
    current.delete()
    newCurrent.renameTo(current)

    /* removing old files */
    val names = files map { _.getName }
    names
      .filter { _ != "current" }
      .foreach { name => new File(home, name).delete() }

    block
  }

  val log = new DataOutputStream(new FileOutputStream(new File(home, "log")))

  def put(block: NamesBlock) {
    NamesBlockSerializer.writeTo(block, log)
    current = current merge block
  }

  def put(info: PathInfo): Unit = put(NamesBlock(info))

  def put(paths: List[PathInfo]): Unit = put(NamesBlock(paths))


  /* TODO: Refactor it*/

  def getPathInfo = current.paths
}
