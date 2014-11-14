package com.github.abulychev.iris.distributed.names

import scala.collection.mutable
import java.io.File
import org.apache.commons.io.FileUtils
import scala.util.Try
import com.github.abulychev.iris.storage.local.names.serializer.NamesBlockSerializer
import com.github.abulychev.iris.storage.local.names.NamesBlock
import com.github.abulychev.iris.dht.actor.Token
import org.apache.commons.codec.binary.Hex
import com.github.abulychev.iris.model.PathInfo

/**
  * User: abulychev
  * Date: 10/21/14
  */
class VersionedNamesStorage(home: File) {
   home.mkdirs()
 
   private val files = home.listFiles().toList
   private val info = mutable.Map.empty[Token, VersionsHolder]
   info ++= files
     .map { file => (stringToToken(file.getName), new VersionsHolder(file)) }
 
   private def tokenToString(token: Token): String = new String(Hex.encodeHex(token.value))
   private def stringToToken(str: String): Token = Token(Hex.decodeHex(str.toCharArray))
 
   def versionOf(token: Token): Option[Long] = {
     info
       .get(token)
       .map { _.getVersion }
   }
 
   def put(token: Token, version: Long, block: NamesBlock) {
     info.getOrElseUpdate(token, new VersionsHolder(new File(home, tokenToString(token))))
       .addVersion(version, block)
   }
 
   def put(token: Token, version: Long, info: PathInfo): Unit =
     put(token, version, NamesBlock(info))
 
   def put(list: UpdateList) {
     list.updates foreach { case ((ep, v), b) => put(ep,v,b) }
   }
 
   def get(list: VersionsList): UpdateList = {
     val versions = list.versions
 
     UpdateList(
       info
         .map { case (t, h) => ((t, h.getVersion), h.afterVersion(versions.getOrElse(t, 0L))) }
         .toMap
     )
   }
   
   def lastVersions: VersionsList = {
     VersionsList(
       info
         .map { case (t, h) => (t, h.getVersion) }
         .toMap
     )
   }
 }


class VersionsHolder(home: File) {
   home.mkdirs()
   private val files = home.listFiles().toList
 
   val versionBlocks = mutable.Map.empty[Long, NamesBlock]
   versionBlocks ++= files
     .map { file => (file.getName.toLong, file) }
     .toMap
     .mapValues { file => FileUtils.readFileToByteArray(file) }
     .mapValues { bytes => Try { NamesBlockSerializer.fromBinary(bytes) } }
     .filter { case (_, b) => b.isSuccess }
     .mapValues { _.get }
 
   private var version: Long = if (versionBlocks.size == 0) 0 else versionBlocks.keys.max
 
   def addVersion(newVersion: Long, block: NamesBlock) = {
     if (newVersion > version) {
       versionBlocks += newVersion -> block
       val data = NamesBlockSerializer.toBinary(block)
       FileUtils.writeByteArrayToFile(new File(home, newVersion.toString), data)
       version = newVersion
     }
   }
 
   def afterVersion(version: Long): NamesBlock = {
     versionBlocks
       .filterKeys { _ > version }
       .values
       .foldLeft(NamesBlock())({ case (a,b) => a merge b })
   }
 
   def getVersion = version
 }