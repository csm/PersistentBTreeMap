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

import java.io.{IOException, OutputStream}
import collection.mutable.ArrayBuffer
import java.nio.{ByteOrder, ByteBuffer}
import java.security.MessageDigest

object PageBuffers
{
  val PAGESIZE = 1024
}

class PageBufferOutputStream(initialBuffers:Option[Array[ByteBuffer]] = None) extends OutputStream
{
  import PageBuffers._
  private val bufs:ArrayBuffer[ByteBuffer] = new ArrayBuffer[ByteBuffer]()
  if (initialBuffers.isDefined)
    bufs ++= initialBuffers.get.map(b => b.duplicate().order(ByteOrder.BIG_ENDIAN))
  else
    bufs += ByteBuffer.allocate(PAGESIZE)
  private val md = MessageDigest.getInstance("SHA-1")
  private var closed = false

  def write(b: Int) = {
    val a = new Array[Byte](1)
    a(0) = b.toByte
    write(a, 0, 1)
  }

  override def write(b:Array[Byte], offset:Int, length:Int):Unit = {
    if (closed)
      throw new IOException("closed output stream")
    val buf = bufs.last
    val rem = buf.remaining() - md.getDigestLength
    val n = Math.min(rem, length)
    if (n <= rem)
    {
      buf.put(b, offset, length)
    }
    else
    {
      if (n > 0)
        buf.put(b, offset, n)
      md.update(buf.duplicate().flip().asInstanceOf[ByteBuffer])
      buf.put(md.digest())
      val nextBuf = ByteBuffer.allocate(PAGESIZE).order(ByteOrder.BIG_ENDIAN)
      nextBuf.putInt(PageType.Continuation)
      bufs += nextBuf
      write(b, offset+n, length-n)
    }
  }

  override def close() {
    if (bufs.last.remaining() > md.getDigestLength)
    {
      val buf = bufs.last
      buf.position(PAGESIZE - md.getDigestLength)
      md.update(buf.duplicate().flip().asInstanceOf[ByteBuffer])
      buf.put(md.digest())
    }
    closed = true
  }

  def buffers():Array[ByteBuffer] = {
    bufs.map(b => b.duplicate().flip().asInstanceOf[ByteBuffer].asReadOnlyBuffer()).toArray
  }
}
