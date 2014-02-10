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
import java.util.Iterator;
import java.util.UUID;

public class CacheManager {

  private final StorageManager storageManager;
  private final MappingTable table = new MappingTable();

  public CacheManager(StorageManager storageManager) {
    if (storageManager == null) throw new IllegalArgumentException();

    this.storageManager = storageManager;
  }

  public InMemoryPage createInMemoryPage() {
    InMemoryPage page = new InMemoryPage();
    table.put(page);

    return page;
  }

  public OnDiskPage createOnDiskPage(UUID pid) {
    if (pid == null) throw new IllegalArgumentException();

    OnDiskPage page = new OnDiskPage(pid);
    table.put(page);

    return page;
  }

  public InMemoryPage getInMemoryPage(UUID pid) throws IOException {
    if (pid == null) throw new IllegalArgumentException();

    Page page = table.get(pid);

    return page.getInMemoryPage(this);
  }

  public InMemoryPage getInMemoryPage(OnDiskPage onDiskPage) throws IOException {
    InMemoryPage page = storageManager.read(onDiskPage);
    table.put(page);

    return page;
  }

  public OnDiskPage getOnDiskPage(UUID pid) {
    if (pid == null) throw new IllegalArgumentException();

    Page page = table.get(pid);
    if (page != null) {
      return page.getOnDiskPage(this);
    } else {
      return null;
    }
  }

  public void flush() throws IOException {
    Iterator<Page> iterator = table.iterator();
    while (iterator.hasNext()) {
      Page page = iterator.next();
      page.flush(storageManager);
    }
  }

}
