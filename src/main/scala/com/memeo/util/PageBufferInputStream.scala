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

import java.io.InputStream
import java.nio.ByteBuffer
import java.security.MessageDigest

class PageBufferInputStream(val buffers:List[ByteBuffer]) extends InputStream
{
  private val md = MessageDigest.getInstance("SHA-1")
  private var b = buffers.map(b => b.duplicate().position(4).limit(PageBuffers.PAGESIZE - md.getDigestLength).asInstanceOf[ByteBuffer]).toList

  def read(): Int = {
    if (b.isEmpty)
      -1
    else if (b.head.remaining > 0)
      b.head.get() & 0xFF
    else
    {
      b = b.tail
      read()
    }
  }
}
