package com.github.abulychev.iris.distributed.info.actor

import akka.actor.ActorRef
import java.net.InetSocketAddress
import com.github.abulychev.iris.storage.local.info.actor.FileInfoStorage
import com.github.abulychev.iris.storage.local.info.serialize.FileContentInfoSerializer
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage.Message
import com.github.abulychev.iris.serialize.StringSerializer
import com.github.abulychev.iris.model.FileContentInfo
import com.github.abulychev.iris.InfoService

/**
 * User: abulychev
 * Date: 10/23/14
 */
class DistributedFileInfoStorage(storage: ActorRef,
                                 routingService: ActorRef,
                                 registry: ActorRef)
  extends DistributedStorage(
    storage,
    routingService,
    InfoService,
    StringSerializer,
    FileContentInfoSerializer,
    registry) {

  def translateIntoMessage = PartialFunction[Any, Message] {
    case FileInfoStorage.Get(id) => DistributedStorage.Get(id)
    case FileInfoStorage.Put(id, info) => DistributedStorage.Put(id, info)
    case FileInfoStorage.Response(result) => DistributedStorage.Response(result)
    case FileInfoStorage.Acknowledged => DistributedStorage.Acknowledged
    case FileInfoStorage.GetAllKeys => DistributedStorage.GetAllKeys
    case FileInfoStorage.AllKeys(keys) => DistributedStorage.AllKeys(keys)

    case InfoRoutingActor.AddRoute(key) => DistributedStorage.AddRoute(key)
    case InfoRoutingActor.GetRoutes(key) => DistributedStorage.GetRoutes(key)
    case InfoRoutingActor.RoutesResponse(routes) => DistributedStorage.RoutesResponse(routes)
  }

  def translateFromMessage = PartialFunction[Message, Any] {
    case DistributedStorage.Get(key: String) => FileInfoStorage.Get(key)
    case DistributedStorage.Put(key: String, value: FileContentInfo) => FileInfoStorage.Put(key, value)
    case DistributedStorage.Response(result: Option[FileContentInfo]) => FileInfoStorage.Response(result)
    case DistributedStorage.Acknowledged => FileInfoStorage.Acknowledged
    case DistributedStorage.GetAllKeys => FileInfoStorage.GetAllKeys
    case DistributedStorage.AllKeys(keys) => FileInfoStorage.AllKeys(keys.asInstanceOf[Set[String]])

    case DistributedStorage.AddRoute(key: String) => InfoRoutingActor.AddRoute(key)
    case DistributedStorage.GetRoutes(key: String) => InfoRoutingActor.GetRoutes(key)
    case DistributedStorage.RoutesResponse(routes: List[InetSocketAddress]) => InfoRoutingActor.RoutesResponse(routes)
  }
}
