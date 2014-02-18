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

import java.io.IOException;
import java.util.UUID;

public abstract class InMemoryRecord {

  private final UUID pid;
  private final ByteArray key;

  private boolean isFlushed = false;

  protected InMemoryRecord(UUID pid, ByteArray key) {
    this.pid = pid;
    this.key = key;
  }

  public abstract ByteArray get(InMemoryPage page);

  public abstract void write(StorageManager storageManager) throws IOException;

  public UUID getPid() {
    return pid;
  }

  public ByteArray getKey() {
    return key;
  }

  public boolean isFlushed() {
    return isFlushed;
  }

  public void setFlushed() {
    isFlushed = true;
  }

}
