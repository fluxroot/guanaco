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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Our mapping table. As described for the Bw-Tree, this maps a logical "page identifier" or PID to a page.
 */
public class MappingTable {

  private final Map<UUID, Page> map = new HashMap<>();

  public Page get(UUID pid) {
    if (pid == null) throw new IllegalArgumentException();

    return map.get(pid);
  }

  public void put(Page page) {
    if (page == null) throw new IllegalArgumentException();

    map.put(page.getPid(), page);
  }

  public Iterator<Page> iterator() {
    return map.values().iterator();
  }

}
