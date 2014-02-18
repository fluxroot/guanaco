/*
 * Copyright 2013-2014 the original author or authors.
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
package info.exascale.guanaco;

import com.google.common.primitives.UnsignedBytes;

import java.util.Arrays;

/**
 * This class encapsulates byte[] so we can implement the equals() method.
 */
public class ByteArray implements Comparable<ByteArray> {

  private final byte[] bytes;

  public ByteArray(byte... bytes) {
    if (bytes == null) throw new IllegalArgumentException();

    this.bytes = bytes;
  }

  public byte[] get() {
    return bytes;
  }

  public int length() {
    return bytes.length;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ByteArray)) {
      return false;
    }
    ByteArray rhs = (ByteArray) obj;

    return Arrays.equals(bytes, rhs.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public int compareTo(ByteArray o) {
    if (o == null) throw new IllegalArgumentException();

    return UnsignedBytes.lexicographicalComparator().compare(bytes, o.bytes);
  }

}
