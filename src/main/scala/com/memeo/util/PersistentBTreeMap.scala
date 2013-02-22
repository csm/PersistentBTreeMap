/*
 * Copyright 2013 Memeo, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.memeo.util

import java.io._
import java.util.Comparator
import java.nio.{ByteOrder, ByteBuffer}
import java.nio.channels.{OverlappingFileLockException, FileLock, FileChannel}
import java.security.MessageDigest
import collection.{Map, SortedMapLike, SortedMap}
import collection.immutable.{Stack, TreeMap}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.locks.LockSupport
import java.nio.file.{StandardOpenOption, OpenOption}

class MalformedBTreeException(msg:String) extends IOException(msg)

private class Root(val ptr:Long)
private class KeyValue[K, V](val key:K, val value:V)
{
  def writeTo(out:DataOutput)(implicit ks:Serializer[K], vs:Serializer[V]) = {
    val bos = new ByteArrayOutputStream()
    ks.serialize(bos, key)
    val kb = bos.toByteArray
    out.writeInt(kb.length)
    out.write(kb)
    bos.reset()
    vs.serialize(bos, value)
    val vb = bos.toByteArray
    out.writeInt(vb.length)
    out.write(vb)
  }
}

private class KeyPointer[K](val key:K, val ptr:Long)
{
  def writeTo(out:DataOutput)(implicit ks:Serializer[K]) = {
    val bos = new ByteArrayOutputStream()
    ks.serialize(bos, key)
    val kb = bos.toByteArray
    out.writeInt(kb.length)
    out.write(kb)
    out.writeLong(ptr)
  }
}

private class Node
private case class InternalNode[K](val parts:Array[KeyPointer[K]], val ahead:Long) extends Node
{
  def writeTo(out:DataOutput)(implicit ks:Serializer[K]) = {
    out.writeInt(parts.length)
    parts.foreach(kp => kp.writeTo(out))
    out.writeLong(ahead)
  }
}

private case class LeafNode[K, V](val parts:Array[KeyValue[K, V]]) extends Node
{
  def writeTo(out:DataOutput)(implicit ks:Serializer[K], vs:Serializer[V]) = {
    out.writeInt(parts.length)
    parts.foreach(kv => kv.writeTo(out))
  }
}

class PersistentBTreeMap[K <: AnyRef, V <: AnyRef](val file:File,
                               val keySerializer:Option[Serializer[K]] = None,
                               val valueSerializer:Option[Serializer[V]] = None,
                               val comparator:Option[Comparator[K]] = None,
                               val readOnly:Boolean = false,
                               val fromKey:Option[K] = None,
                               val toKey:Option[K] = None)
  extends scala.collection.mutable.Map[K, V]
{
  private val channel = if (readOnly) new LockableFileChannel(file.getAbsolutePath, StandardOpenOption.READ)
  else new LockableFileChannel(file.getAbsolutePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)

  private val comp:Comparator[K] = comparator.getOrElse(new Comparator[K] {
      def compare(o1: K, o2: K): Int = o1.asInstanceOf[Comparable[K]].compareTo(o2)
    })

  private val kser:Serializer[K] = keySerializer.getOrElse(new Serializer[K] {
    def serialize(out:OutputStream, value:K) {
      val oos = new ObjectOutputStream(out)
      oos.writeObject(value)
      oos.flush()
    }

    def deserialize(in:InputStream):K = {
      val ois = new ObjectInputStream(in)
      ois.readObject().asInstanceOf[K]
    }
  })

  private val vser:Serializer[V] = valueSerializer.getOrElse(new Serializer[V] {
    def serialize(out: OutputStream, value: V) {
      val oos = new ObjectOutputStream(out)
      oos.writeObject(value)
      oos.flush()
    }

    def deserialize(in: InputStream): V = {
      val ois = new ObjectInputStream(in)
      ois.readObject().asInstanceOf[V]
    }
  })

  @volatile private var root = if (channel.size() == 0)
  {
    if (!readOnly)
    {
      val lock = lockFile()
      try
      {
        val pbos = new PageBufferOutputStream(None)
        val os = new DataOutputStream(pbos)
        os.writeInt(PageType.Root)
        os.writeLong(-1)
        os.close()
        channel.write(pbos.buffers)
      }
      finally
      {
        lock.release()
      }
    }
    new Root(-1)
  }
    else readRoot()

  private def validatePage(page:ByteBuffer):Boolean = {
    val md = MessageDigest.getInstance("SHA-1")
    md.update(page.duplicate().limit(PageBuffers.PAGESIZE - md.getDigestLength).asInstanceOf[ByteBuffer])
    val digest = ByteBuffer.wrap(md.digest())
    page.duplicate().position(PageBuffers.PAGESIZE - md.getDigestLength).limit(PageBuffers.PAGESIZE).equals(digest)
  }

  private def readRoot(pos:Long = -1):Root = {
    val numPages = channel.size() / PageBuffers.PAGESIZE
    if (numPages - pos < 0)
      throw new MalformedBTreeException("root page not found")
    try
    {
      val buf = readPage(numPages + pos)
      if (validatePage(buf) && buf.getInt(0) == PageType.Root)
        new Root(buf.getLong(4))
      else
        readRoot(pos - 1)
    }
    catch
    {
      case x:MalformedBTreeException => readRoot(pos - 1)
    }
  }

  /**
   * Read a single page from the file, returning it as a byte buffer.
   *
   * @param pos The *page* position, that is, the real offset will be pos*PAGESIZE.
   * @return The page.
   */
  private def readPage(pos:Long):ByteBuffer = {
    if (pos * PageBuffers.PAGESIZE + PageBuffers.PAGESIZE > channel.size())
      throw new IOException("requested page is not contained in this file's length")
    val buf = ByteBuffer.allocateDirect(PageBuffers.PAGESIZE).order(ByteOrder.BIG_ENDIAN)
    channel.position(pos * PageBuffers.PAGESIZE)
    channel.read(buf)
    if (!validatePage(buf))
      throw new MalformedBTreeException("page " + pos + " had mismatched hash")
    buf
  }

  // mutable.Map

  private def lockFile():FileLock = {
    try
    {
      channel.lock()
    }
    catch
    {
      case ofle:OverlappingFileLockException => {
        LockSupport.parkNanos(100)
        lockFile()
      }
    }
  }

  def +=(kv: (K, V)) = {
    if (readOnly)
      throw new UnsupportedOperationException("immutable map")
    if (fromKey.isDefined && comp.compare(fromKey.get, kv._1) < 0)
      throw new UnsupportedOperationException("key is out of range of this map")
    if (toKey.isDefined && comp.compare(toKey.get, kv._1) >= 0)
      throw new UnsupportedOperationException("key is out of range of this map")
    val lock = lockFile()
    try
    {
      if (root.ptr == -1)
      {
        val leaf = LeafNode[K, V](Array(new KeyValue[K, V](kv._1, kv._2)))
        val pbos = new PageBufferOutputStream(None)
        val dos = new DataOutputStream(pbos)
        dos.writeInt(PageType.Leaf)
        dos.writeInt(1)
        leaf.parts(0).writeTo(dos)
        dos.close()
        val bufs = pbos.buffers()
        val pos = channel.size()
        channel.position(pos)
        channel.write(bufs)
        root = new Root(pos)
        val rpbos = new PageBufferOutputStream(None)
        val rdos = new DataOutputStream(rpbos)
        rdos.writeInt(PageType.Root)
        rdos.writeLong(root.ptr)
        rdos.close()
        channel.write(rpbos.buffers())
      }
      else
      {
        val path = nodePath(kv._1)
        val leaf = path.head
      }
      this
    }
    finally
    {
      lock.release()
    }
  }

  def -=(key: K) = {
    if (readOnly)
      throw new UnsupportedOperationException("immutable map")
    if (fromKey.isDefined && comp.compare(fromKey.get, key) < 0)
      throw new UnsupportedOperationException("key is out of range of this map")
    if (toKey.isDefined && comp.compare(toKey.get, key) >= 0)
      throw new UnsupportedOperationException("key is out of range of this map")
    val lock = lockFile()
    try
    {
      this
    }
    finally
    {
      lock.release()
    }
  }

  def get(key: K): Option[V] = {
    if (fromKey.isDefined && comp.compare(fromKey.get, key) < 0)
      None
    else if (toKey.isDefined && comp.compare(key, toKey.get) >= 0)
      None
    else if (root.ptr < 0)
      None
    else
      find(key, getNode(root.ptr))
  }

  private def find(key: K, node: Node): Option[V] = {
    node match {
      case leaf:LeafNode[K, V] => {
        leaf.parts.find(kv => kv.key.eq(key)).map(kv => kv.value)
      }
      case node:InternalNode[K] => {
        val until = node.parts.find(kp => comp.compare(key, kp.key) <= 0)
        if (!until.isDefined) find(key, getNode(node.ahead))
        else find(key, getNode(until.get.ptr))
      }
    }
  }

  private def nodePath(key: K, from:Node=getNode(root.ptr)): List[Node] = {
    from match {
      case i:InternalNode[K] => {
        i.parts.find(kp => comp.compare(key, kp.key) <= 0) match {
          case s:Some[KeyPointer[K]] => i :: nodePath(key, getNode(s.get.ptr))
          case None => i :: nodePath(key, getNode(i.ahead))
        }
      }

      case l:LeafNode[K, V] => List(l)
    }
  }

  def iterator: Iterator[(K, V)] = valueStream().toIterator

  private def continuations(ptr:Long):List[ByteBuffer] = {
    if (ptr * PageBuffers.PAGESIZE + PageBuffers.PAGESIZE > channel.size())
      List()
    else
    {
      val page = readPage(ptr)
      page.getInt(0) match {
        case PageType.Continuation => page :: continuations(ptr + 1)
        case _ => List(page)
      }
    }
  }

  private def getNode(ptr:Long):Node = {
    val pages = readPage(ptr) :: continuations(ptr + 1)
    val in = new DataInputStream(new PageBufferInputStream(pages))
    val count = in.readUnsignedShort()
    pages.head.getInt(0) match {
      case PageType.Leaf => {
        LeafNode[K, V]((0 until count).map(_ => {
          val keyBytes = new Array[Byte](in.readInt())
          in.readFully(keyBytes)
          val k = kser.deserialize(new ByteArrayInputStream(keyBytes))
          val valueBytes = new Array[Byte](in.readInt())
          in.readFully(valueBytes)
          val v = vser.deserialize(new ByteArrayInputStream(valueBytes))
          new KeyValue[K, V](k, v)
        }).toArray)
      }
      case PageType.Node => {
        InternalNode[K]((0 until count).map(_ => {
          val keyBytes = new Array[Byte](in.readInt())
          in.readFully(keyBytes)
          val k = kser.deserialize(new ByteArrayInputStream(keyBytes))
          val ptr = in.readLong()
          new KeyPointer[K](k, ptr)
        }).toArray, in.readLong())
      }
      case x => throw new MalformedBTreeException("invalid page tag: " + x)
    }
  }

  private def valueStream(node:Node = getNode(root.ptr)): Stream[(K, V)] = {
    node match {
      case leaf:LeafNode[K, V] => {
        if (leaf.parts.isEmpty) Stream.empty[(K, V)]
        else {
          if (fromKey.isDefined && comp.compare(leaf.parts.head.key, fromKey.get) < 0)
            valueStream(LeafNode(leaf.parts.tail))
          else if (toKey.isDefined && comp.compare(toKey.get, leaf.parts.head.key) >= 0)
            Stream.empty[(K, V)]
          else
            leaf.parts.map(kv => (kv.key, kv.value)).toStream
        }
      }
      case inode:InternalNode[K] => {
        if (inode.parts.isEmpty) valueStream(getNode(inode.ahead))
        else {
          if (fromKey.isDefined && comp.compare(inode.parts.head.key, fromKey.get) < 0)
            valueStream(InternalNode(inode.parts.tail, inode.ahead))
          else if (toKey.isDefined && comp.compare(toKey.get, inode.parts.head.key) >= 0)
            Stream.empty[(K, V)]
          else
            valueStream(getNode(inode.parts.head.ptr)) #::: valueStream(InternalNode(inode.parts.tail, inode.ahead))
        }
      }
    }
  }

  // SortedMapLike-like methods
  // Note that we can't seem to extend SortedMapLike itself, because it looks like it
  // conflicts with mutable.Map

  implicit def ordering: Ordering[K] = Ordering.comparatorToOrdering(comp)

  def range(from: Option[K], until: Option[K]): PersistentBTreeMap[K, V] =
    new PersistentBTreeMap[K, V](file, keySerializer, valueSerializer, comparator, readOnly, from, until)

  def head(until:K): PersistentBTreeMap[K, V] = range(None, Some(until))
  def tail(from:K): PersistentBTreeMap[K, V] = range(Some(from), None)

  def close():Unit = {
    if (channel.isOpen())
    {
      channel.close()
      fp.close()
    }
  }

  override def finalize() {
    close()
    super.finalize()
  }
}
