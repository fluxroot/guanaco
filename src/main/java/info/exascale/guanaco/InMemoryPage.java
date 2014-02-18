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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

public class InMemoryPage extends Page {

  private HashMap<ByteArray, InMemoryPageRecord> records = new HashMap<>();
  private HashMap<ByteArray, InMemoryRecord> deltas = new HashMap<>();

  public InMemoryPage() {
    super();
  }

  public InMemoryPage(UUID pid) {
    super(pid);
  }

  @Override
  public InMemoryPage getInMemoryPage(CacheManager cacheManager) {
    return this;
  }

  @Override
  public OnDiskPage getOnDiskPage(CacheManager cacheManager) {
    return null;
  }

  @Override
  public void flush(StorageManager storageManager) throws IOException {
    storageManager.write(this);
  }

  public ByteArray get(ByteArray key) {
    if (key == null) throw new IllegalArgumentException();

    // Look up the key in the deltas first
    InMemoryRecord record = deltas.get(key);
    if (record == null) {
      // We haven't found it there, check the records
      record = records.get(key);
    }

    if (record != null) {
      // Return the value depending on the record type
      return record.get(this);
    } else {
      return null;
    }
  }

  public void put(ByteArray key, ByteArray value) {
    if (key == null) throw new IllegalArgumentException();
    if (value == null) throw new IllegalArgumentException();

    // Check whether we have already a record
    InMemoryRecord record = records.get(key);
    if (record != null) {
      // Insert a modify record
      deltas.put(key, new InMemoryModifyRecord(getPid(), key, value));
    } else {
      // We found no record
      deltas.put(key, new InMemoryInsertRecord(getPid(), key, value));
    }
  }

  public void delete(ByteArray key) {
    if (key == null) throw new IllegalArgumentException();

    // Check the deltas first if a record exists
    InMemoryRecord record = deltas.get(key);
    if (record == null) {
      // Check the records
      record = records.get(key);
    }

    if (record != null) {
      deltas.put(key, new InMemoryDeleteRecord(getPid(), key));
    }
  }

  public ByteArray get(InMemoryPageRecord record) {
    return record.getValue();
  }

  public ByteArray get(InMemoryInsertRecord record) {
    return record.getValue();
  }

  public ByteArray get(InMemoryModifyRecord record) {
    return record.getValue();
  }

  public ByteArray get(InMemoryDeleteRecord record) {
    return null;
  }

  public Collection<InMemoryPageRecord> getPageRecords() {
    Collection<InMemoryPageRecord> collection = new ArrayList<>();

    for (InMemoryPageRecord record : records.values()) {
      if (!record.isFlushed()) {
        collection.add(record);
      }
    }

    return collection;
  }

  public Collection<InMemoryRecord> getDeltasToFlush() {
    Collection<InMemoryRecord> collection = new ArrayList<>();

    for (InMemoryRecord record : deltas.values()) {
      if (!record.isFlushed()) {
        collection.add(record);
      }
    }

    return collection;
  }

  public void add(InMemoryPageRecord record) {
    records.put(record.getKey(), record);
  }

  public void add(InMemoryRecord inMemoryRecord) {
    deltas.put(inMemoryRecord.getKey(), inMemoryRecord);
  }

}
