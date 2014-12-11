package com.github.abulychev.iris.application.nomad

/**
 * User: abulychev
 * Date: 12/10/14
 */
sealed trait Action

case object Idle extends Action

case object Root extends Action
case class ChangeDirectory(name: String) extends Action
case object Up extends Action

case class MakeDirectory(name: String) extends Action
case class RemoveDirectory(name: String) extends Action

case class Write(name: String, content: Array[Byte]) extends Action
case class Delete(name: String) extends Action
