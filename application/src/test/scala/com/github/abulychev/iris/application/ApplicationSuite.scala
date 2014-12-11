package com.github.abulychev.iris.application

import org.scalatest.{BeforeAndAfter, FunSuite}
import java.io.File
import org.apache.commons.io.FileUtils
import com.github.abulychev.iris.application.nomad._
import akka.actor.ActorSystem
import scala.collection.mutable

/**
 * User: abulychev
 * Date: 12/12/14
 */
class ApplicationSuite extends FunSuite with BeforeAndAfter {
  import Directories._

  val cb = new ConfigBuilder(10340, 11340, 12340, List(0,1))
  val tmp = new File("/tmp/iris")
  val control = new File(tmp, "control")

  val systems = mutable.Map.empty[Int, ActorSystem]

  def config(i: Int) = cb.build(i)
  def mount(i: Int) = new File(tmp, s"mount$i")
  
  def run(i: Int): ActorSystem = {
    val home = new File(tmp, s"data$i"); home.mkdir()
    mount(i).mkdir()

    val system = ApplicationBuilder.build(mount(i), home, config(i))
    systems += i -> system

    Thread.sleep(10000)
    system
  }

  def stop(i: Int) {
    systems(i).shutdown()
    systems -= i
    Thread.sleep(3000)
  }

  def stopAll() {
    systems.values.foreach(_.shutdown())
    systems.clear()
  }

  before {
    FileUtils.deleteDirectory(tmp)
    tmp.mkdirs()
    control.mkdir()
  }

  after {
    stopAll()
    Thread.sleep(5000)
    FileUtils.deleteDirectory(tmp)
  }

  test("one node runs and re-runs") {
    run(0)

    val state = NomadFactory.nomad(mount(0)).apply(30)
    Thread.sleep(3000)
    NomadFactory.repeat(control, state.history).force

    stop(0)

    run(0)

    assert(equal(mount(0), control))

    stop(0)
  }


  test("two nodes should synchronize") {
    run(0)

    val stream = NomadFactory.nomad(mount(0)).drop(30)
    Thread.sleep(3000)

    run(1)

    stream.drop(15)
    Thread.sleep(10000)

    assert(equal(mount(0), mount(1)))

    stop(0)
    stop(1)
  }

  test("four nodes should synchronize") {
    def compareAll() {
      assert(equal(mount(0), mount(1)))
      assert(equal(mount(0), mount(2)))
      assert(equal(mount(0), mount(3)))
      assert(equal(mount(1), mount(2)))
      assert(equal(mount(1), mount(3)))
      assert(equal(mount(2), mount(3)))
    }

    run(0)
    run(1)
    run(2)

    NomadFactory.nomad(mount(0)).take(20).force

    run(3)

    Thread.sleep(10000)

    compareAll()

    NomadFactory.nomad(mount(3)).take(20).force
    Thread.sleep(10000)

    compareAll()

    stop(0)
    stop(1)
    stop(2)
    stop(3)
  }
}
