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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.UUID;

public class StorageManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

  private static final byte PAGE_RECORD = 1;
  private static final byte INSERT_RECORD = 2;
  private static final byte MODIFY_RECORD = 3;
  private static final byte DELETE_RECORD = 4;

  private final RandomAccessFile db;

  public StorageManager(File file) throws FileNotFoundException {
    if (file == null) throw new IllegalArgumentException();

    db = new RandomAccessFile(file, "rw");
  }

  @Override
  public void close() throws IOException {
    db.close();
  }

  public void initialize(CacheManager cacheManager, IIndex index) throws IOException {
    // Reset position so we're reading from the end
    long position = db.length();
    db.seek(position);

    while (position > 0) {
      // Read the type of the next bytes
      --position;
      db.seek(position);
      byte type = db.readByte();
      db.seek(position);

      // Read the UUID of the page
      UUID pid = readUUID(position);
      position = db.getFilePointer();
      OnDiskPage page = cacheManager.getOnDiskPage(pid);
      if (page == null) {
        page = cacheManager.createOnDiskPage(pid);
      }

      ByteArray key = readByteArray(position);
      position = db.getFilePointer();

      index.add(key, page);

      switch (type) {
        case PAGE_RECORD:
          OnDiskPageRecord record = new OnDiskPageRecord(position, pid, key);
          page.add(record);
          skipByteArray(position);
          position = db.getFilePointer();
          break;
        case INSERT_RECORD:
          OnDiskInsertRecord insertRecord = new OnDiskInsertRecord(position, pid, key);
          page.add(insertRecord);
          skipByteArray(position);
          position = db.getFilePointer();
          break;
        case MODIFY_RECORD:
          OnDiskModifyRecord modifyRecord = new OnDiskModifyRecord(position, pid, key);
          page.add(modifyRecord);
          skipByteArray(position);
          position = db.getFilePointer();
          break;
        case DELETE_RECORD:
          OnDiskDeleteRecord deleteRecord = new OnDiskDeleteRecord(position, pid, key);
          page.add(deleteRecord);
          break;
        default:
          // Something's not right
          throw new IOException();
      }
    }
  }

  private void skipByteArray(long position) throws IOException {
    position -= 4;
    db.seek(position);
    int length = db.readInt();
    db.seek(position);
    position -= length;
    db.seek(position);
  }

  public InMemoryPage read(OnDiskPage page) throws IOException {
    if (page == null) throw new IllegalArgumentException();

    InMemoryPage inMemoryPage = new InMemoryPage(page.getPid());

    for (OnDiskPageRecord record : page.getPageRecords()) {
      InMemoryPageRecord pageRecord = read(record);
      inMemoryPage.add(pageRecord);
    }

    for (OnDiskRecord record : page.getDeltas()) {
      InMemoryRecord inMemoryRecord = record.accept(this);
      inMemoryPage.add(inMemoryRecord);
    }

    return inMemoryPage;
  }

  public InMemoryPageRecord read(OnDiskPageRecord record) throws IOException {
    ByteArray value = readByteArray(record.getPosition());

    return new InMemoryPageRecord(record.getPid(), record.getKey(), value);
  }

  public InMemoryRecord read(OnDiskInsertRecord record) throws IOException {
    ByteArray value = readByteArray(record.getPosition());

    return new InMemoryInsertRecord(record.getPid(), record.getKey(), value);
  }

  public InMemoryRecord read(OnDiskModifyRecord record) throws IOException {
    ByteArray value = readByteArray(record.getPosition());

    return new InMemoryModifyRecord(record.getPid(), record.getKey(), value);
  }

  public InMemoryRecord read(OnDiskDeleteRecord record) throws IOException {
    return new InMemoryDeleteRecord(record.getPid(), record.getKey());
  }

  private ByteArray readByteArray(long position) throws IOException {
    position -= 4;
    db.seek(position);
    int length = db.readInt();
    db.seek(position);
    position -= length;
    db.seek(position);
    byte[] bytes = new byte[length];
    int read = db.read(bytes);
    db.seek(position);

    if (read != length) {
      throw new IOException("Error reading bytes");
    }

    return new ByteArray(bytes);
  }

  private UUID readUUID(long position) throws IOException {
    position -= 8;
    db.seek(position);
    long leastSignificantBits = db.readLong();
    db.seek(position);
    position -= 8;
    db.seek(position);
    long mostSignificantBits = db.readLong();
    db.seek(position);

    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  public void write(InMemoryPage page) throws IOException {
    db.seek(db.length());

    Collection<InMemoryPageRecord> pageRecords = page.getPageRecords();
    for (InMemoryPageRecord record : pageRecords) {
      record.write(this);
      record.setFlushed();
    }

    Collection<InMemoryRecord> records = page.getDeltasToFlush();
    for (InMemoryRecord record : records) {
      record.write(this);
      record.setFlushed();
    }
  }

  public void write(InMemoryPageRecord record) throws IOException {
    write(record.getValue());
    write(record.getKey());
    write(record.getPid());
    db.writeByte(PAGE_RECORD);
  }

  public void write(InMemoryInsertRecord record) throws IOException {
    write(record.getValue());
    write(record.getKey());
    write(record.getPid());
    db.writeByte(INSERT_RECORD);
  }

  public void write(InMemoryModifyRecord record) throws IOException {
    write(record.getValue());
    write(record.getKey());
    write(record.getPid());
    db.writeByte(MODIFY_RECORD);
  }

  public void write(InMemoryDeleteRecord record) throws IOException {
    write(record.getKey());
    write(record.getPid());
    db.writeByte(DELETE_RECORD);
  }

  private void write(ByteArray byteArray) throws IOException {
    db.write(byteArray.get());
    db.writeInt(byteArray.length());
  }

  private void write(UUID uuid) throws IOException {
    db.writeLong(uuid.getMostSignificantBits());
    db.writeLong(uuid.getLeastSignificantBits());
  }

}
