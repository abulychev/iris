package com.github.abulychev.iris.gossip.node

import scala.collection.mutable

/**
 * User: abulychev
 * Date: 9/26/14
 */
class ArrivalWindow {
  private val stats = new StatsWindow(ArrivalWindow.SampleSize)
  private var tLast: Long = 0L
  
  def add(tNow: Long) {
    val interval = if (tLast == 0) ArrivalWindow.DefaultArrivalInterval else tNow - tLast
    if (interval < ArrivalWindow.MaxInterval) stats.add(interval)
    tLast = tNow
  }
  
  def phi(tNow: Long): Double = {
    require(tLast > 0 && stats.size > 0)

    val interval = tNow - tLast
    ArrivalWindow.PhiFactor * interval / stats.mean
  }
}


object ArrivalWindow {
  val SampleSize = 100
  val DefaultArrivalInterval = 2000
  val MaxInterval = 10000
  val PhiFactor: Double = 1.0 / Math.log(10.0)
}


class StatsWindow(sampleSize: Int) {
  private val queue = mutable.Queue.empty[Long]
  private var _sum: Long = 0L

  def add(value: Long) {
    _sum += value
    queue.enqueue(value)
    if (queue.size > sampleSize) {
      _sum -= queue.dequeue()
    }
  }

  def size = queue.size
  def sum: Long = _sum
  def mean: Double = sum / size
}

