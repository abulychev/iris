package com.github.abulychev.iris.filesystem

import akka.actor.ActorRef

/**
 * User: abulychev
 * Date: 2/9/15
 */
object Filesystem {
  /* Commands */
  trait Command

  case class Open(path: String) extends Command
  case class Create(path: String) extends Command

  case class GetAttributes(path: String) extends Command

  case class MakeDirectory(path: String) extends Command
  case class ReadDirectory(path: String) extends Command

  case class Remove(path: String)


  /* Answers */
  trait Answer

  case class Opened(actor: ActorRef) extends Answer
  case class Created(actor: ActorRef) extends Answer
  case class Attributes(entities: List[Entity])

  trait Entity {
    def name: String
    def ct: Long
  }
  case class FileEntity(name: String, size: Long, ct: Long) extends Entity
  case class DirectoryEntity(name: String, ct: Long) extends Entity
}
