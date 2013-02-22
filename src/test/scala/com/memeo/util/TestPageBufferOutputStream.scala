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

import org.junit.Assert._
import org.junit.Test
import java.io.DataOutputStream
import java.security.MessageDigest
import java.nio.ByteBuffer

class TestPageBufferOutputStream
{
  @Test def testEmpty() {
    val o = new PageBufferOutputStream(None)
    o.close()
    val b = o.buffers()
    assertEquals(1, b.length)
    val md = MessageDigest.getInstance("SHA-1")
    (0 until (PageBuffers.PAGESIZE - md.getDigestLength)).foreach(_ => md.update(0.toByte))
    val d1 = ByteBuffer.wrap(md.digest())
    b(0).position(PageBuffers.PAGESIZE - md.getDigestLength)
    assertEquals(d1, b(0))
  }

  @Test def testRoot() {
    val o = new PageBufferOutputStream(None)
    val d = new DataOutputStream(o)
    d.writeInt(PageType.Root)
    d.writeLong(-1)
    d.close()
    val b = o.buffers()
    assertEquals(1, b.length)
    assertEquals(PageBuffers.PAGESIZE, b(0).remaining())
    assertEquals(PageType.Root, b(0).getInt(0))
    assertEquals(-1, b(0).getLong(4))
  }

  @Test def testSplit() {
    val o = new PageBufferOutputStream(None)
  }
}
