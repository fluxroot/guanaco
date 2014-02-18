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

public abstract class Page {

  private final UUID pid;

  protected Page() {
    pid = UUID.randomUUID();
  }

  protected Page(UUID pid) {
    if (pid == null) throw new IllegalArgumentException();

    this.pid = pid;
  }

  public UUID getPid() {
    return pid;
  }

  public abstract InMemoryPage getInMemoryPage(CacheManager cacheManager) throws IOException;

  public abstract OnDiskPage getOnDiskPage(CacheManager cacheManager);

  public abstract void flush(StorageManager storageManager) throws IOException;

}
