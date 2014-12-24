package com.github.abulychev.iris.util.rpc.tcp

import akka.actor._
import java.net.InetSocketAddress
import scala.collection.mutable
import com.github.abulychev.iris.util.rpc.{RpcMessage, Rpc}
import scala.concurrent.duration._

/**
 * User: abulychev
 * Date: 12/16/14
 */
class ClientSession(endpoint: InetSocketAddress, handler: ActorRef, idleTimeout: Duration) extends Actor with ActorLogging {
  import ClientSession._

  val reqid2actor = mutable.Map.empty[Long, ActorRef]
  val actor2reqid = mutable.Map.empty[ActorRef, Long]
  var terminating = false

  val connection = context.actorOf(Props(classOf[ClientConnection], endpoint, self))
  context.watch(connection)

  context.setReceiveTimeout(idleTimeout)

  def receive = {
    /* From client */
    case r @ Rpc.Request(bytes) =>
      val reqid = generate
      val task = context.actorOf(Props(classOf[ClientTcpTask], sender(), connection, r, reqid))

      reqid2actor += reqid -> task
      actor2reqid += task -> reqid

      context.watch(task)

    case Rpc.Push(bytes) =>
      connection ! RpcMessage(0, bytes, first = true, last = true)

    case Rpc.Response(_) =>
      log.error("Client sent response as first message")

    /* From socket */
    case RpcMessage(0, _, _, _) =>
      log.error("Client received Rpc.Push message")

    case RpcMessage(reqid, _, _, _) if !reqid2actor.contains(reqid) =>
      log.error(s"Client does not waiting message with request id $reqid. May be timeout was reached")

    case msg @ RpcMessage(reqid, bytes, _, _) =>
      val actor = reqid2actor(reqid)
      actor ! msg

    /* Auxiliary */
    case Terminated(actor) if actor != connection =>
      val reqid = actor2reqid(actor)
      actor2reqid -= actor
      reqid2actor -= reqid

      if (actor2reqid.size == 0 && terminating) context.stop(self)

    case Terminated(actor) if actor == connection =>
      //log.error(s"Unexpectedly closing of socket for connection: $endpoint")
      context.stop(self)

    case ReceiveTimeout =>
      log.info(s"Reached idle timeout for connection: $endpoint")
      context.setReceiveTimeout(Duration.Undefined)
      handler ! Detach

    case Acknowledged =>
      if (actor2reqid.size == 0) context.stop(self)
      else terminating = true
  }

  private def generate = RpcMessage.generateId(reqid2actor.keySet.toSet)
}

object ClientSession {
  case object Detach
  case object Acknowledged

  def props(endpoint: InetSocketAddress,
            handler: ActorRef,
            idleTimeout: Duration = 5.seconds): Props =
    Props(classOf[ClientSession],
      endpoint,
      handler,
      idleTimeout
    )
}