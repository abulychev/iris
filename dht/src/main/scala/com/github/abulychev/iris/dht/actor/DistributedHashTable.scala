package com.github.abulychev.iris.dht.actor

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import java.net.InetSocketAddress
import java.util
import com.github.abulychev.iris.dht.storage.{Row, Atom}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Failure
import scala.collection.mutable
import com.github.abulychev.iris.dht.actor.remote.{StorageHandler, StorageClient}
import scala.concurrent.duration.Duration

/**
 * User: abulychev
 * Date: 10/9/14
 */
class DistributedHashTable(localAddress: InetSocketAddress,
                           token: Token) extends Actor with ActorLogging {

  import DistributedHashTable._

  val connections =  mutable.Map.empty[InetSocketAddress, ActorRef]
  val localStorage = context.actorOf(Storage.props, "storage")

  val ordering = new Ordering[Put] {
    def compare(x: Put, y: Put): Int = -java.lang.Long.compare(x.expireTime, y.expireTime)
  }
  val values = mutable.Map.empty[InetSocketAddress, mutable.PriorityQueue[Put]]
  values += localAddress -> mutable.PriorityQueue.empty[Put](ordering)

  def storage(endpoint: InetSocketAddress): ActorRef = {
    if (endpoint == localAddress) localStorage
    else connections(endpoint)
  }

  val nodes = new util.TreeMap[Token, InetSocketAddress]
  nodes += token -> localAddress

  def find(token: Token, n: Int): List[InetSocketAddress] = {
    if (nodes.size() <= n) nodes.values.toList
    else {
      val buffer = ListBuffer[InetSocketAddress]()
      var it = nodes.tailMap(token).entrySet.iterator
      while (buffer.size < n) {
        val e = if (it.hasNext) it.next() else {
          it = nodes.entrySet.iterator
          it.next()
        }
        buffer += e.getValue
      }
      buffer.toList
    }
  }

  def prev(token: Token): Token = {
    if (nodes.size() == 1) token
    else {
      val entries = nodes.headMap(token)
      if (entries.size() > 0) entries.lastKey() else nodes.lastKey()
    }
  }

  override def preStart() {
    context.actorOf(StorageHandler.props(localStorage, localAddress), "handler")
  }

  def receive = {
    case DistributedHashTable.PutBatch(batch) =>
      val groups = batch
        .flatMap { case put => find(put.clusterKey, ReplicaFactor)
          .map { put -> _ }
        }
        .groupBy { case (_, ep) => ep }
        .mapValues { _.map { case (list, _) => list }}

      groups
        .foreach { case (ep, list) =>
          val pq = values(ep)
          val time = System.currentTimeMillis
          while (pq.nonEmpty && pq.head.expireTime < time) pq.dequeue()
          pq ++= list
        }

      groups
        .mapValues { _.map { _.toLocal }}
        .foreach { case (ep, list) => storage(ep) ! Storage.PutBatch(list) }


    case put @ DistributedHashTable.Put(_, _, _, _) =>
      self forward DistributedHashTable.PutBatch(List(put))


    case DistributedHashTable.Get(clusterKey, tableName, row) =>
      val req = sender()
      val storages = find(clusterKey, ReplicaFactor) map storage

      context.actorOf(Props(new Actor {
        storages foreach { _ ! Storage.Get(tableName, row) }

        def receive = waiting(storages.size, false, Set())

        def waiting(n: Int, ok: Boolean, result: Set[Row]): Receive = {
          if (n == 0) {
            if (!ok) req ! Failure(new Exception)
            if (ok) req ! DistributedHashTable.Response(result.toList)
            context.stop(self)
          }

          PartialFunction[Any,Unit] {
            case ack @ Storage.Response(rows) =>
              context.become(waiting(n-1, true, result ++ rows))

            case Failure(_) =>
              context.become(waiting(n-1, ok, result))
          }
        }
      }))

    case Up(endpoint, token) =>
      log.info(s"Up endpoint: $endpoint")
      nodes += token -> endpoint

      val connection = context.actorOf(StorageClient.props(endpoint))
      connections += endpoint -> connection

      /* Put values */
      val pq = mutable.PriorityQueue.empty[Put](ordering)
      values += endpoint -> pq

      val pEndpoint = nodes(prev(token))
      val batch = PutBatch(values(pEndpoint).toList)
      pq ++= batch.batch
      connection ! batch

    case Down(endpoint, token) =>
      log.info(s"Down endpoint: $endpoint")
      nodes -= token

      context.stop(connections(endpoint))
      connections -= endpoint

      /* Resend values */
      val pq = values(endpoint)
      val time = System.currentTimeMillis
      while (pq.nonEmpty && pq.head.expireTime < time) pq.dequeue()
      val batch = PutBatch(pq.toList)
      self ! batch
      values -= endpoint
  }
}

object DistributedHashTable {
  def props(localAddress: InetSocketAddress, token: Token): Props =
    Props(classOf[DistributedHashTable], localAddress, token)

  case class PutBatch(batch: List[Put])

  case class Put(clusterKey: Token, tableName: String, row: Row, expireTime: Long) {
    def toLocal: Storage.Put = Storage.Put(tableName, row, expireTime)
  }
  object Put {
    def apply(tableName: String, row: Row, ttl: Duration): Put = apply(
      defaultClusterKey(row),
      tableName,
      row,
      expireTime(ttl)
    )
  }

  case class Get(clusterKey: Token, tableName: String, row: Row)
  object Get {
    def apply(tableName: String, row: Row): Get = apply(
      defaultClusterKey(row),
      tableName,
      row
    )
  }

  case class Response(rows: List[Row])

  case class Up(endpoint: InetSocketAddress, token: Token)
  case class Down(endpoint: InetSocketAddress, token: Token)

  private def defaultClusterKey(row: Row) = Token(row.columns.head.value)

  val ReplicaFactor = 2

  private def expireTime(ttl: Duration) = System.currentTimeMillis() + ttl.toMillis
}

case class Token(value: Array[Byte]) extends Atom with Ordered[Token] {
  def compare(that: Token) = this compare that.asInstanceOf[Atom]
}