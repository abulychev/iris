package com.github.abulychev.iris.distributed.cluster

import java.io.File
import org.apache.commons.io.FileUtils
import com.github.abulychev.iris.serialize.IntSerializer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.gossip.serialize.GenerationSerializer

/**
 * User: abulychev
 * Date: 10/29/14
 */
class GenerationHolder(home: File) {
  private val file = new File(home, "generation")
  private val tmp = new File(home, ".generation")

  if (!file.exists() && tmp.exists()) {
    tmp.renameTo(file)
  }

  private val old = if (file.exists()) {
    val bytes = FileUtils.readFileToByteArray(file)
     IntSerializer.fromBinary(bytes)
  } else 0

  private val generation = old + 1

  private val bytes = new GenerationSerializer().toBinary(generation)
  FileUtils.writeByteArrayToFile(tmp, bytes)
  tmp.renameTo(file)
  tmp.delete()

  def get: Generation = generation
}
