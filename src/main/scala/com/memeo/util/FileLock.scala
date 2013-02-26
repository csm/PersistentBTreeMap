/*
 * Copyright 2013 Casey Marshall.
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

import java.util.concurrent.locks.{ReentrantLock, Lock}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel, FileLock, FileChannel}
import collection.mutable
import java.nio.channels.FileChannel.MapMode
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.file.{Paths, Path, OpenOption}
import ref.SoftReference
import collection.convert.WrapAsJava

private[util] object FileLocks
{
  private val _locks_lock = new ReentrantLock()
  private val _locks = new mutable.HashMap[String, SoftReference[Lock]]()

  private[util] def getLock(p:String):Lock = {
    val path = Paths.get(p).toAbsolutePath().normalize().toString()
    _locks_lock.lock()
    try
    {
      if (!_locks.contains(path))
      {
        val lock = new ReentrantLock()
        _locks += (path -> new SoftReference[Lock](lock))
        lock
      }
      else
      {
        val ref:Option[SoftReference[Lock]] = _locks.get(path)
        if (ref.isDefined && ref.get.get.isDefined)
        {
          ref.get.get.get
        }
        else
        {
          val lock = new ReentrantLock()
          _locks += (path -> new SoftReference[Lock](lock))
          lock
        }
      }
    }
    finally
    {
      _locks_lock.unlock()
    }
  }
}

/**
 * A {@see FileChannel} that provides locking support between threads as well as between
 * processes. This means that if multiple threads attempt to lock the same file (regions
 * are ignored) that they will first need to acquire a shared
 *
 * @param path
 * @param option
 */
class LockableFileChannel(val path:String, val option:OpenOption*) extends FileChannel
{
  private val channel:FileChannel = FileChannel.open(Paths.get(path), WrapAsJava.setAsJavaSet(option.toSet))

  def read(dst: ByteBuffer): Int = channel.read(dst)

  def read(dsts: Array[ByteBuffer], offset: Int, length: Int): Long = channel.read(dsts, offset, length)

  def write(src: ByteBuffer): Int = channel.write(src)

  def write(srcs: Array[ByteBuffer], offset: Int, length: Int): Long = channel.write(srcs, offset, length)

  def position(): Long = channel.position()

  def position(newPosition: Long): FileChannel = channel.position(newPosition)

  def size(): Long = channel.size()

  def truncate(size: Long): FileChannel = channel.truncate(size)

  def force(metaData: Boolean) = channel.force(metaData)

  def transferTo(position: Long, count: Long, target: WritableByteChannel): Long = channel.transferTo(position, count, target)

  def transferFrom(src: ReadableByteChannel, position: Long, count: Long): Long = channel.transferFrom(src, position, count)

  def read(dst: ByteBuffer, position: Long): Int = channel.read(dst, position)

  def write(src: ByteBuffer, position: Long): Int = channel.write(src, position)

  def map(mode: MapMode, position: Long, size: Long): MappedByteBuffer = channel.map()

  private class ComboLock(val lock:Lock, val fileLock:FileLock, channel:FileChannel, pos:Long, size:Long, shared:Boolean) extends FileLock(channel, pos, size, shared)
  {
    def isValid: Boolean = fileLock.isValid

    def release() = {
      lock.unlock()
      fileLock.release()
    }
  }

  def lock(position: Long, size: Long, shared: Boolean): FileLock = {
    val l = FileLocks.getLock(path)
    l.lock()
    return new ComboLock(l, channel.lock(position, size, shared), channel, position, size, shared)
  }

  def tryLock(position: Long, size: Long, shared: Boolean): FileLock = {
    val l = FileLocks.getLock(path)
    if (l.tryLock())
    {
      val ll = Option(channel.tryLock(position, size, shared))
      if (ll.isDefined)
      {
        return new ComboLock(l, ll.get, channel, position, size, shared)
      }
      else
      {
        l.unlock()
        null
      }
    }
    else null
  }

  def implCloseChannel() = channel.close()
}