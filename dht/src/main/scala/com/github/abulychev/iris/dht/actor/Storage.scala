package com.github.abulychev.iris.dht.actor

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import com.github.abulychev.iris.dht.storage.{Row, Table}
import com.github.abulychev.iris.dht.actor.Storage._
import scala.collection.mutable
import scala.concurrent.duration._

/**
 * User: abulychev
 * Date: 10/7/14
 */
private[dht] class Storage private extends Actor with ActorLogging {
  val tables = mutable.Map.empty[String, ActorRef]
  def table(name: String) = tables.getOrElseUpdate(name, context.actorOf(Props(classOf[TableActor])))

  def receive = {
    case put @ Put(tableName, _, _) =>
      table(tableName) forward put

    case PutBatch(batch) =>
      batch
        .groupBy { _.tableName }
        .foreach { case (tableName, list) => table(tableName) ! PutBatch(list) }

    case get @ Get(tableName, _) =>
      table(tableName) forward get

    case msg => log.error(s"Unhandled message: $msg")
  }
}

object Storage {
  def props = Props(classOf[Storage])

  case class Put(tableName: String, row: Row, expireTime: Long)
  case class PutBatch(batch: List[Put])
  case class Get(tableName: String, row: Row)
  case class Response(rows: List[Row])
}

class TableActor private extends Actor with ActorLogging {
  val table = new Table

  object RemoveObsolete
  context.system.scheduler.schedule(5 minutes, 5 minutes, self, RemoveObsolete)(context.dispatcher)

  def receive = {
    case Put(_, row, expireTime) =>
      table.put(row, expireTime)

    case PutBatch(batch) =>
      batch foreach { case Put(_, row, expireTime) => table.put(row, expireTime) }

    case Get(_, row) =>
      sender ! Response(table.get(row))

    case RemoveObsolete =>
      table.removeObsolete()
  }
}