package com.github.abulychev.iris.util.rpc

import akka.actor.{Actor, Props, ActorSystem}
import java.net.InetSocketAddress
import ClientBuilder.client
import akka.util.ByteString
import scala.concurrent.duration._
import com.github.abulychev.iris.util.rpc.tcp.{Server, ClientManager}


/**
 * User: abulychev
 * Date: 12/16/14
 */
object RpcTest extends App {
  val system = ActorSystem()

  val echo = system.actorOf(Props(new Actor {
    def receive = {
      case Rpc.Request(bytes) =>
        val seq = new String(bytes.toArray).toInt

        if (seq == 10) sender ! Rpc.Response(ByteString("stop".getBytes))
        else sender ! Rpc.Request(bytes, 5.seconds)

      case Rpc.Timeout =>
        println("2 timeout")


      case Rpc.Push(bytes) =>
        println("push: " + new String(bytes.toArray))
    }
  }))

  val address = new InetSocketAddress("localhost", 12345)

  /* TCP */
  val server = system.actorOf(Props(classOf[Server], address, echo), "server")
  val clients = system.actorOf(Props(classOf[ClientManager]), "clients")

  /* Udp */
//  val server = system.actorOf(Props(classOf[UdpSocket], address, echo), "server")
//  val clients = system.actorOf(Props(classOf[UdpSocket], new InetSocketAddress("localhost", 12346), Actor.noSender), "clients")

  implicit val cb = ClientBuilder(clients)

  val simple  = system.actorOf(Props(new Actor {
    client(address) ! Rpc.Request(ByteString(new String("1").getBytes), 1.second)

    client(address) ! Rpc.Push(ByteString(new String("teeest").getBytes))

    def receive = {
      case Rpc.Request(bytes) =>
        val seq = new String(bytes.toArray).toInt + 1
        println(s"sending $seq")
        Thread.sleep(3000)
        sender ! Rpc.Request(ByteString(seq.toString.getBytes), 1.seconds)

      case Rpc.Response(bytes) =>
        println(new String(bytes.toArray))

      case Rpc.Timeout =>
        println(":(")
    }
  }))

  Thread.sleep(30000)
  system.shutdown()
}
