package com.github.abulychev.iris.util.rpc.udp

import akka.actor._
import java.net.InetSocketAddress
import akka.io.{Udp, IO}
import com.github.abulychev.iris.util.rpc.{WriteTo, Rpc, RpcMessage, RpcMessageSerializer}
import scala.collection.mutable
import akka.util.ByteString

/**
 * User: abulychev
 * Date: 12/24/14
 */
class UdpSocket(endpoint: InetSocketAddress, handler: ActorRef) extends Actor with ActorLogging {
  val reqid2actor = mutable.Map.empty[(InetSocketAddress, Long), ActorRef]
  val actor2reqid = mutable.Map.empty[ActorRef, (InetSocketAddress, Long)]

  import context.system
  IO(Udp) ! Udp.Bind(self, endpoint)

  val queue = mutable.Queue.empty[(ActorRef, Any)]

  def receive = {
    case _: Udp.Bound =>
      queue.foreach { case (actor, msg) => self.tell(msg, actor) }
      queue.clear()

      context.become(ready(sender()))

    case _: Udp.CommandFailed =>
      context.stop(self)

    case msg => queue.enqueue(sender() -> msg)
  }

  def ready(socket: ActorRef): Receive = {
    case Udp.Received(bytes, remote) =>
      val msg = RpcMessageSerializer.fromBinary(bytes.toArray)

      msg match {
        case RpcMessage(0, data, _, _) =>
          handler ! Rpc.Push(data)

        case RpcMessage(reqid, _, true, _) if reqid2actor.contains((remote, reqid)) =>
          log.error("Message is first, but request id already exists")

        case msg @ RpcMessage(reqid, _, true, _) =>
          val task = context.actorOf(Props(classOf[ServerUdpTask], handler, socket, remote, msg, reqid))
          reqid2actor += (remote, reqid) -> task
          actor2reqid += task -> (remote, reqid)

          context.watch(task)

        case RpcMessage(reqid, _, false, _) if !reqid2actor.contains((remote, reqid)) =>
          log.error("Message is not first, but request id does not exist")

        case msg @ RpcMessage(reqid, _, false, _) =>
          val task = reqid2actor((remote, reqid))
          task ! msg
      }

    case WriteTo(remote, Rpc.Push(bytes)) =>
      val message = RpcMessage(0, bytes, first = true, last = true)
      val data = ByteString(RpcMessageSerializer.toBinary(message))
      socket ! Udp.Send(data, remote)

    case WriteTo(remote, req: Rpc.Request) =>
      val reqid = generate(remote)
      val task = context.actorOf(Props(classOf[ClientUdpTask], sender(), socket, remote, req, reqid))
      reqid2actor += (remote, reqid) -> task
      actor2reqid += task -> (remote, reqid)

      context.watch(task)

    case Terminated(actor) =>
      val (remote, reqid) = actor2reqid(actor)
      actor2reqid -= actor
      reqid2actor -= ((remote, reqid))
  }

  private def generate(endpoint: InetSocketAddress): Long =
    RpcMessage.generateId(reqid2actor.keys
      .filter { case (e, _) => e == endpoint }
      .map { case (_, i) => i }
      .toSet
    )
}
