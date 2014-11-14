package com.github.abulychev.iris.distributed.cluster

import java.io.File
import org.apache.commons.io.FileUtils
import com.github.abulychev.iris.dht.actor.Token

/**
 * User: abulychev
 * Date: 10/29/14
 */
class TokenHolder(home: File) {
  private val file = new File(home, "token")
  private val tmp = new File(home, ".token")

  private val token = if (file.exists()) {
    val bytes = FileUtils.readFileToByteArray(file)
    Token(bytes)
  } else generate

  private def generate: Token = {
    val bytes = new Array[Byte](TokenHolder.TokenLength)
    util.Random.nextBytes(bytes)
    FileUtils.writeByteArrayToFile(tmp, bytes)
    tmp.renameTo(file)
    tmp.delete()
    Token(bytes)
  }

  def get = token
}

object TokenHolder {
  val TokenLength = 20
}
