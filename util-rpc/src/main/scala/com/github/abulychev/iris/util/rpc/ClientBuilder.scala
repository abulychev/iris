package com.github.abulychev.iris.util.rpc

import akka.actor.{Actor, ActorContext, ActorRef}
import java.net.InetSocketAddress

/**
 * User: abulychev
 * Date: 12/16/14
 */
case class ClientBuilder(manager: ActorRef)

object ClientBuilder {
  def client(endpoint: InetSocketAddress)(implicit cb: ClientBuilder) =
    ActorRefLike(cb.manager, endpoint)
}

case class ActorRefLike(pool: ActorRef, endpoint: InetSocketAddress) {
  def !(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit = {
    pool.tell(WriteTo(endpoint, message), sender)
  }
  def tell(msg: Any, sender: ActorRef): Unit = this.!(msg)(sender)
  def forward(message: Any)(implicit context: ActorContext) = tell(message, context.sender())

}
