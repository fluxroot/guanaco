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
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

public class OnDiskPage extends Page {

  private HashMap<ByteArray, OnDiskPageRecord> records = new HashMap<>();
  private HashMap<ByteArray, OnDiskRecord> deltas = new HashMap<>();

  public OnDiskPage(UUID pid) {
    super(pid);
  }

  @Override
  public InMemoryPage getInMemoryPage(CacheManager cacheManager) throws IOException {
    return cacheManager.getInMemoryPage(this);
  }

  @Override
  public OnDiskPage getOnDiskPage(CacheManager cacheManager) {
    return this;
  }

  @Override
  public void flush(StorageManager storageManager) {
    // Do nothing
  }

  public Collection<OnDiskPageRecord> getPageRecords() {
    return records.values();
  }

  public Collection<OnDiskRecord> getDeltas() {
    return deltas.values();
  }

  public void add(OnDiskPageRecord record) {
    records.put(record.getKey(), record);
  }

  public void add(OnDiskInsertRecord insertRecord) {
    OnDiskRecord record = deltas.get(insertRecord.getKey());
    if (record == null) {
      deltas.put(insertRecord.getKey(), insertRecord);
    }
  }

  public void add(OnDiskModifyRecord modifyRecord) {
    OnDiskRecord record = deltas.get(modifyRecord.getKey());
    if (record == null) {
      deltas.put(modifyRecord.getKey(), modifyRecord);
    }
  }

  public void add(OnDiskDeleteRecord deleteRecord) {
    OnDiskRecord record = deltas.get(deleteRecord.getKey());
    if (record == null) {
      deltas.put(deleteRecord.getKey(), deleteRecord);
    }
  }

}
