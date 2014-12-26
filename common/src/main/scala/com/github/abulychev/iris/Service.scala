package com.github.abulychev.iris

import akka.actor.ActorRef

/**
 * User: abulychev
 * Date: 12/25/14
 */
sealed case class Service(code: Byte)

object GossipService extends Service(1)
object DhtService extends Service(2)
object NameService extends Service(3)
object InfoService extends Service(4)
object ChunkService extends Service(5)

case class RegisterService(service: Service, actor: ActorRef)
