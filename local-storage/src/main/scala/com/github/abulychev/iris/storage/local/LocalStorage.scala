package com.github.abulychev.iris.storage.local

/**
  * User: abulychev
  * Date: 3/13/14
  */
object LocalStorage {
   val ChunkSize = 4 * 1024 * 1024

   def createChunkRanges(size: Int) = {
     val count = (size + ChunkSize - 1) / ChunkSize
     (0 to (count - 1)).map(i => (i*ChunkSize) to Math.min((i+1)*ChunkSize - 1, size - 1))
   }
 }
