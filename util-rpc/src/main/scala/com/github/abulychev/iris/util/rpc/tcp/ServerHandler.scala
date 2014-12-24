package com.github.abulychev.iris.util.rpc.tcp

import akka.actor._
import scala.concurrent.duration._
import com.github.abulychev.iris.util.rpc.Rpc
import scala.collection.mutable
import com.github.abulychev.iris.util.rpc.RpcMessage

/**
 * User: abulychev
 * Date: 12/17/14
 */
class ServerHandler(delegate: ActorRef, connection: ActorRef, idleTimeout: FiniteDuration) extends Actor with ActorLogging {
  val reqid2actor = mutable.Map.empty[Long, ActorRef]
  val actor2reqid = mutable.Map.empty[ActorRef, Long]

  context.setReceiveTimeout(idleTimeout)

  def receive = {
    case RpcMessage(0, bytes, _, _) =>
      delegate ! Rpc.Push(bytes)

    case msg @ RpcMessage(reqid, _, true, _) if reqid2actor.contains(reqid) =>
      log.error("Message is first, but request id already exists")

    case msg @ RpcMessage(reqid, _, true, _) =>
      val task = context.actorOf(Props(classOf[ServerTcpTask], delegate, connection, msg, reqid))

      actor2reqid += task -> reqid
      reqid2actor += reqid -> task

      context.watch(task)

    case msg @ RpcMessage(reqid, _, false, _) if !reqid2actor.contains(reqid) =>
      log.error("Message is not first, but request id does not exist")

    case msg @ RpcMessage(reqid, _, false, _) =>
      val task = reqid2actor(reqid)
      task ! msg

    case Terminated(actor) =>
      val reqid = actor2reqid(actor)
      actor2reqid -= actor
      reqid2actor -= reqid

    case ReceiveTimeout =>
      context.stop(context.parent)
  }
}
