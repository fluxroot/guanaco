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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * HashMapIndex maps a key to a pid. For now this is just a one-to-one mapping.
 * So every page contains exactly one key.
 */
public class HashMapIndex implements IIndex {

  private final CacheManager cacheManager;
  private final Map<ByteArray, UUID> map = new HashMap<>();

  public HashMapIndex(CacheManager cacheManager) {
    if (cacheManager == null) throw new IllegalArgumentException();

    this.cacheManager = cacheManager;
  }

  @Override
  public ByteArray get(ByteArray key) throws IOException {
    if (key == null) throw new IllegalArgumentException();

    ByteArray value = null;

    // Get the page for the key
    UUID pid = map.get(key);
    if (pid != null) {
      // We have found the page. Lets get the InMemoryPage from the CacheManager.
      InMemoryPage page = cacheManager.getInMemoryPage(pid);
      value = page.get(key);
    }

    return value;
  }

  @Override
  public void put(ByteArray key, ByteArray value) throws IOException {
    if (key == null) throw new IllegalArgumentException();
    if (value == null) throw new IllegalArgumentException();

    InMemoryPage page;

    // Get the page for the key
    UUID pid = map.get(key);
    if (pid == null) {
      // We have no page for this key. Lets create one and install it in our map.
      page = cacheManager.createInMemoryPage();
      map.put(key, page.getPid());
    } else {
      // We have found the page. Lets get the InMemoryPage from the CacheManager.
      page = cacheManager.getInMemoryPage(pid);
    }
    page.put(key, value);
  }

  @Override
  public void delete(ByteArray key) throws IOException {
    if (key == null) throw new IllegalArgumentException();

    // Get the page for the key
    UUID pid = map.get(key);
    if (pid != null) {
      // We have found the page. Lets get the InMemoryPage from the CacheManager.
      InMemoryPage page = cacheManager.getInMemoryPage(pid);
      page.delete(key);
    }
  }

  @Override
  public void add(ByteArray key, OnDiskPage page) {
    if (key == null) throw new IllegalArgumentException();
    if (page == null) throw new IllegalArgumentException();

    map.put(key, page.getPid());
  }

}
