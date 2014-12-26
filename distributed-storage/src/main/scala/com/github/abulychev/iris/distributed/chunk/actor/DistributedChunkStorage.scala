package com.github.abulychev.iris.distributed.chunk.actor

import akka.actor.ActorRef
import java.net.InetSocketAddress
import com.github.abulychev.iris.storage.local.chunk.actor.ChunkStorage
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage.Message
import com.github.abulychev.iris.serialize.{ByteArraySerializer, StringSerializer}
import com.github.abulychev.iris.ChunkService

/**
  * User: abulychev
  * Date: 10/23/14
  */
class DistributedChunkStorage(storage: ActorRef,
                              routingService: ActorRef,
                              registry: ActorRef)
  extends DistributedStorage(
       storage,
       routingService,
       ChunkService,
       StringSerializer,
       ByteArraySerializer,
       registry) {

  def translateIntoMessage = PartialFunction[Any, Message] {
    case ChunkStorage.Get(id) => DistributedStorage.Get(id)
    case ChunkStorage.Put(id, data) => DistributedStorage.Put(id, data)
    case ChunkStorage.Response(result) => DistributedStorage.Response(result)
    case ChunkStorage.Acknowledged => DistributedStorage.Acknowledged
    case ChunkStorage.GetAllKeys => DistributedStorage.GetAllKeys
    case ChunkStorage.AllKeys(keys) => DistributedStorage.AllKeys(keys)

    case ChunkRoutingActor.AddRoute(key) => DistributedStorage.AddRoute(key)
    case ChunkRoutingActor.GetRoutes(key) => DistributedStorage.GetRoutes(key)
    case ChunkRoutingActor.RoutesResponse(routes) => DistributedStorage.RoutesResponse(routes)
  }

  def translateFromMessage = PartialFunction[Message, Any] {
    case DistributedStorage.Get(key: String) => ChunkStorage.Get(key)
    case DistributedStorage.Put(key: String, value: Array[Byte]) => ChunkStorage.Put(key, value)
    case DistributedStorage.Response(result: Option[Array[Byte]]) => ChunkStorage.Response(result)
    case DistributedStorage.Acknowledged => ChunkStorage.Acknowledged
    case DistributedStorage.GetAllKeys => ChunkStorage.GetAllKeys
    case DistributedStorage.AllKeys(keys) => ChunkStorage.AllKeys(keys.asInstanceOf[Set[String]])

    case DistributedStorage.AddRoute(key: String) => ChunkRoutingActor.AddRoute(key)
    case DistributedStorage.GetRoutes(key: String) => ChunkRoutingActor.GetRoutes(key)
    case DistributedStorage.RoutesResponse(routes: List[InetSocketAddress]) => ChunkRoutingActor.RoutesResponse(routes)
  }

}