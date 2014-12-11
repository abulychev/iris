package com.github.abulychev.iris.application

import com.typesafe.config.{Config, ConfigFactory}

/**
 * User: abulychev
 * Date: 12/11/14
 */
class ConfigBuilder(gossip: Int, dht: Int, storage: Int, seeds: List[Int]) {
  val s = seeds
    .map(_ + gossip)
    .map(_.toString)
    .map("localhost:" + _)
    .map("\"" + _ + "\"")
    .mkString("[", ", ", "]")

  def build(i: Int): Config = {
    ConfigFactory.parseString(
      s"""
        |com.github.abulychev.iris {
        |  host = "localhost"
        |  gossip {
        |    port = ${gossip + i}
        |    seeds = $s
        |  }
        |
        |  dht {
        |    port = ${dht + i}
        |  }
        |
        |  storage {
        |    port = ${storage + i}
        |  }
        |}
      """.stripMargin)
  }
}
