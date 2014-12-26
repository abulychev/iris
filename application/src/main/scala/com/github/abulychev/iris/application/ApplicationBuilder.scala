package com.github.abulychev.iris.application

import java.io.File
import akka.actor._
import com.github.abulychev.iris.localfs.actor.FSActor
import com.github.abulychev.iris.storage.local.names.actor.NameNode
import java.net.{URI, InetAddress, InetSocketAddress}
import com.github.abulychev.iris.dht.actor.DistributedHashTable
import com.github.abulychev.iris.storage.local.info.actor.FileInfoStorage
import com.github.abulychev.iris.storage.local.chunk.actor.{TemporalStorage, ChunkStorage}
import com.github.abulychev.iris.distributed.info.actor.{DistributedFileInfoStorage, InfoRoutingActor}
import com.github.abulychev.iris.distributed.names.actor.{VersionsRoutingActor, DistributedNamesStorage}
import com.github.abulychev.iris.distributed.cluster.actor.ClusterNode
import com.github.abulychev.iris.distributed.cluster.{TokenHolder, GenerationHolder}
import com.github.abulychev.iris.distributed.routing.RoutingService
import com.github.abulychev.iris.localfs.{LocalFS, FSLogging}
import com.github.abulychev.iris.distributed.chunk.actor.{DistributedChunkStorage, ChunkRoutingActor}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import collection.JavaConversions._

/**
 * User: abulychev
 * Date: 3/12/14
 */
object ApplicationBuilder {
  val log = LoggerFactory.getLogger(ApplicationBuilder.getClass)

  def build(mountPoint: File, home: File): ActorSystem = {
    log.info(s"Loading application with home: {${home.getAbsolutePath}} and mount point: {${mountPoint.getAbsolutePath}}")

    val file = new File(home, "application.conf")
    if (!file.exists() || !file.isFile) {
      log.error(s"Could not find configuration file: {${file.getAbsolutePath}}")
      System.exit(1)
    }

    log.info("Loading configuration ...")
    val config = ConfigFactory.parseFile(file)

    build(mountPoint, home, config)
  }

  def build(mountPoint: File, home: File, appConfig: Config): ActorSystem = {
    val config = appConfig.withFallback(ConfigFactory.load()).getConfig("com.github.abulychev.iris")

    val host = InetAddress.getByName(config.getString("host"))
    val gossipPort = config.getInt("gossip.port")
    val dhtPort=  config.getInt("dht.port")
    val storagePort=  config.getInt("storage.port")

    val seeds = config.getStringList("gossip.seeds")
      .toList
      .map { parse }

    build(
      mountPoint.getAbsolutePath,
      home,
      host,
      gossipPort,
      dhtPort,
      storagePort,
      seeds
    )
  }

  def build(mountPoint: String,
            home: File,
            host: InetAddress,
            gossipPort: Int,
            dhtPort: Int,
            port: Int,
            seeds: List[InetSocketAddress]): ActorSystem = {

    val gossipAddress = new InetSocketAddress(host, gossipPort)
    val namesServiceAddress = new InetSocketAddress(host, port)
    val dhtAddress = new InetSocketAddress(host, dhtPort)

    val generation = new GenerationHolder(home).get
    val token = new TokenHolder(home).get

    val system = ActorSystem("fs")
    system.actorOf(Props(new Actor {
      /* Making of cluster */
      val clusterNode = system.actorOf(ClusterNode.props(gossipAddress, generation, seeds, self, dhtPort, port, token), "cluster")

      /* dht support */
      val dht = system.actorOf(DistributedHashTable.props(dhtAddress, token))

      /* Routing services via dht */
      val versionsRouting = context.actorOf(Props(classOf[VersionsRoutingActor], namesServiceAddress, dht), "versions-routing")
      val infoRouting = context.actorOf(Props(classOf[InfoRoutingActor], namesServiceAddress, dht), "info-routing")
      val chunkRouting = context.actorOf(Props(classOf[ChunkRoutingActor], namesServiceAddress, dht), "chunk-routing")

      val routingService = context.actorOf(Props(classOf[RoutingService], versionsRouting, infoRouting, chunkRouting), "routing-service")

      val aggregator = context.actorOf(Props(classOf[ServicesAggregator], self, 3))

      /* Storages */
      val infoStorage = context.actorOf(FileInfoStorage.props(new File(home, "info")))
      val dInfoStorage = context.actorOf(Props(classOf[DistributedFileInfoStorage],
        infoStorage,
        routingService,
        aggregator
      ))

      val chunkStorage = context.actorOf(ChunkStorage.props(new File(home, "chunks")))
      val dChunkStorage = context.actorOf(Props(classOf[DistributedChunkStorage],
        chunkStorage,
        routingService,
        aggregator
      ))

      val temporal = context.actorOf(Props(new TemporalStorage(new File(home, "temporal"), dChunkStorage)))

      val namenode = system.actorOf(NameNode.props(new File(home, "names"), dInfoStorage), "names-storage")
      val dNamesStorage = system.actorOf(DistributedNamesStorage.props(new File(home, "versions"), namenode, routingService, namesServiceAddress, token, self, aggregator), "distributed-names-storage")

      Thread.sleep(1000)

      val fsActor = system.actorOf(Props(new FSActor(dChunkStorage, temporal, dNamesStorage)))

      val fs = new LocalFS(fsActor, dNamesStorage) with FSLogging

      new Thread(new Runnable { def run() {
        fs.mount(mountPoint)
      }}).start()

      Runtime.getRuntime.addShutdownHook(new Thread { override def run() {
        fs.unmount()
      }})

      def receive = {
        case msg @ ClusterNode.VersionUpdated(token, version) =>
          dNamesStorage ! DistributedNamesStorage.VersionDiscovered(token, version)

        case DistributedNamesStorage.UpdateVersion(version) =>
          clusterNode ! ClusterNode.UpdateVersion(version)

        case ClusterNode.ReachableDht(endpoint, token) =>
          dht ! DistributedHashTable.Up(endpoint, token)

        case ClusterNode.UnreachableDht(endpoint, token) =>
          dht ! DistributedHashTable.Down(endpoint, token)

        case ClusterNode.ReachableData(endpoint) =>
          routingService ! RoutingService.Reachable(endpoint)

        case ClusterNode.UnreachableData(endpoint) =>
          routingService ! RoutingService.Unreachable(endpoint)

        case ServicesAggregator.Collected(services) =>
          context.actorOf(TcpHandler.props(new InetSocketAddress(host, port), services), "tcp")
      }

      override def postStop() {
        fs.unmount()
      }
    }))

    system
  }

  private def parse(address: String): InetSocketAddress = {
    val uri = new URI("http://" + address)
    new InetSocketAddress(uri.getHost, uri.getPort)
  }
}
