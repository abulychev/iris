package com.github.abulychev.iris.application

import java.io.File
import org.slf4j.LoggerFactory


/**
 * User: abulychev
 * Date: 3/19/14
 */
object Main extends App {
  val log = LoggerFactory.getLogger(this.getClass)

  log.info("Starting application ...")

  if (args.length < 2) {
    log.error("Usage: run.sh [home] [mount point]")
    System.exit(1)
  }

  val home = new File(args(0))
  val mountPoint = new File(args(1))

  if (!mountPoint.exists() || !mountPoint.isDirectory) {
    log.error("Mount point should exist")
    System.exit(1)
  }

  if (mountPoint.listFiles.length != 0) {
    log.error("Mount point directory should be empty")
    System.exit(1)
  }

  ApplicationBuilder.build(mountPoint, home)
}
