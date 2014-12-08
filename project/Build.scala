import sbt._
import Keys._

object BuildSettings {
  val buildOrganization = "com.github.abulychev"
  val buildVersion      = "0.0.1"
  val buildScalaVersion = "2.10.3"

  val sharedSettings = Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion
  )
}

// TODO: group them
object Dependencies {
  val apachecodec = "commons-codec" % "commons-codec" % "1.6"
  val apacheio = "commons-io" % "commons-io" % "2.4"

  val guava = "com.google.guava" % "guava" % "18.0"
  val jsr305 = "com.google.code.findbugs" % "jsr305" % "3.0.0"

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.3.0"
  val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % "2.3.0"

  val sprayCan = "io.spray" % "spray-can" % "1.3.1"
  val sprayRouting = "io.spray" % "spray-routing" % "1.3.1"

  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.2" % "test"

  val logback = Seq (
    "ch.qos.logback" % "logback-classic" % "1.1.2",
    "ch.qos.logback" % "logback-core" % "1.1.2"
  )
}

object IrisBuild extends Build {
  import Dependencies._
  import BuildSettings._

  lazy val model = Project (
    id = "model",
    base = file("model"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies += apachecodec
      )
  )

  lazy val common = Project (
    id = "common",
    base = file("common"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies ++= Seq(
          akkaActor, sprayCan, sprayRouting
        )
      )
  )

  lazy val localStorage = Project (
    id = "local-storage",
    base = file("local-storage"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies ++= Seq(
          apacheio, guava, jsr305, akkaActor, akkaSlf4j,
          scalaTest
        )
      )
  ) dependsOn(model, common)

  lazy val fuseJna = ProjectRef (
    id = "fuse-jna",
    base = file("fuse-jna")
  )

  lazy val fs = Project (
    id = "local-fs",
    base = file("local-fs"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies ++= Seq(
          akkaActor, akkaSlf4j
        )
      )
  ) dependsOn(fuseJna, localStorage)

//  lazy val singleNode = Project (
//    id = "single-node",
//    base = file("single-node"),
//    settings = Defaults.defaultSettings ++
//      sharedSettings
//  ) aggregate(model, common, localStorage, fs)

  lazy val gossip = Project (
    id = "gossip",
    base = file("gossip"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies ++= Seq(
          akkaActor, akkaSlf4j
        )
      )
  ) dependsOn common

  lazy val dht = Project (
    id = "dht",
    base = file("dht"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies ++= Seq(
          akkaActor, akkaSlf4j, sprayCan, sprayRouting
        )
      )
  ) dependsOn common

  lazy val distributedStorage = Project (
    id = "distributed-storage",
    base = file("distributed-storage"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies ++= Seq(
          akkaActor, akkaSlf4j, sprayCan, sprayRouting
        )
      )
  ) dependsOn(common, localStorage, gossip, dht)

//  lazy val multiNode = Project (
//    id = "multi-node",
//    base = file("multi-node"),
//    settings = Defaults.defaultSettings ++
//      sharedSettings
//  ) aggregate(singleNode, gossip, dht, distributedStorage)

  lazy val application = Project (
    id = "application",
    base = file("application"),
    settings = Defaults.defaultSettings ++
      sharedSettings ++
      Seq (
        exportJars := true,
        libraryDependencies ++= Seq(
          akkaActor, akkaSlf4j, sprayCan, sprayRouting
        ),
        libraryDependencies ++= logback
      )
  ) dependsOn (model, common, localStorage, fuseJna, fs, gossip, dht, distributedStorage)

  /* Assembly section */

  lazy val assembly = Project (
    id = "assembly",
    base = file("assembly"),
    settings = Defaults.defaultSettings ++
      sharedSettings
  ) dependsOn application
}

