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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class GuanacoStorageEngine implements Closeable {

  private static final String defaultFilename = "guanaco.db";

  private final IIndex index;
  private final CacheManager cacheManager;
  private final StorageManager storageManager;

  public static GuanacoStorageEngine createDatabase() throws IOException {
    return createDatabase(defaultFilename);
  }

  public static GuanacoStorageEngine createDatabase(String filename) throws IOException {
    if (filename == null) throw new IllegalArgumentException();

    return createDatabase(new File(filename));
  }

  public static GuanacoStorageEngine createDatabase(File file) throws IOException {
    if (file == null) throw new IllegalArgumentException();

    if (file.exists()) {
      if (file.isFile()) {
        if (!file.delete()) {
          throw new IOException(String.format("Cannot delete database %s", file));
        }
      } else {
        throw new IOException(String.format("%s is not a file", file));
      }
    }

    return new GuanacoStorageEngine(file);
  }

  public static GuanacoStorageEngine openDatabase() throws IOException {
    return openDatabase(defaultFilename);
  }

  public static GuanacoStorageEngine openDatabase(String filename) throws IOException {
    if (filename == null) throw new IllegalArgumentException();

    return openDatabase(new File(filename));
  }

  public static GuanacoStorageEngine openDatabase(File file) throws IOException {
    if (file == null) throw new IllegalArgumentException();

    return new GuanacoStorageEngine(file);
  }

  private GuanacoStorageEngine(File file) throws IOException {
    if (file == null) throw new IllegalArgumentException();

    storageManager = new StorageManager(file);
    cacheManager = new CacheManager(storageManager);
    index = new HashMapIndex(cacheManager);

    try {
      storageManager.initialize(cacheManager, index);
    } catch (IOException e) {
      storageManager.close();
      throw e;
    }
  }

  public ByteArray get(ByteArray key) throws IOException {
    if (key == null) throw new IllegalArgumentException();

    return index.get(key);
  }

  public void put(ByteArray key, ByteArray value) throws IOException {
    if (key == null) throw new IllegalArgumentException();
    if (value == null) throw new IllegalArgumentException();

    index.put(key, value);
  }

  public void delete(ByteArray key) throws IOException {
    if (key == null) throw new IllegalArgumentException();

    index.delete(key);
  }

  public void flush() throws IOException {
    cacheManager.flush();
  }

  @Override
  public void close() throws IOException {
    flush();
    storageManager.close();
  }

}
