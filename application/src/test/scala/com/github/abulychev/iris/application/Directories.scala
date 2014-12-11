package com.github.abulychev.iris.application

import java.io.File
import org.apache.commons.io.FileUtils
import java.util

/**
 * User: abulychev
 * Date: 12/12/14
 */
object Directories {
  def equal(f1: File, f2: File): Boolean = (f1.isFile, f2.isFile) match {
    case (true, true) =>
      util.Arrays.equals(FileUtils.readFileToByteArray(f1), FileUtils.readFileToByteArray(f2))
    case (false, false) =>
      val files1 = f1.listFiles().toList.map(f => f.getName).toSet
      val files2 = f2.listFiles().toList.map(f => f.getName).toSet
      (files1 == files2) && files1.forall { name => equal(new File(f1, name), new File(f2, name)) }
    case _ => false
  }
}
