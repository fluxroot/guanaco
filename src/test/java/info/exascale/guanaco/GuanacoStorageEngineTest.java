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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GuanacoStorageEngineTest {

  private static final Logger LOG = LoggerFactory.getLogger(GuanacoStorageEngineTest.class);

  private File file = new File("guanaco.db");

  @Before
  public void setUp() {
    if (file.exists() && !file.delete()) {
      LOG.error("Cannot delete database");
    }
  }

  @After
  public void tearDown() {
    if (file.exists() && !file.delete()) {
      LOG.error("Cannot cleanup database");
    }
  }

  @Test
  public void testCreateDatabase() throws IOException {
    Random random = new Random();

    try (GuanacoStorageEngine guanacoStorageEngine = GuanacoStorageEngine.createDatabase(file)) {
      byte[] keyBytes = new byte[16];
      random.nextBytes(keyBytes);
      ByteArray key = new ByteArray(keyBytes);

      byte[] valueBytes = new byte[256];
      random.nextBytes(valueBytes);
      ByteArray value = new ByteArray(valueBytes);

      guanacoStorageEngine.put(key, value);

      assertEquals(value, guanacoStorageEngine.get(key));

      guanacoStorageEngine.delete(key);

      assertNull(guanacoStorageEngine.get(key));
    }
  }

  @Test(expected = IOException.class)
  public void testCreateDatabaseException() throws IOException {
    GuanacoStorageEngine.createDatabase(".");
  }

  @Test
  public void testOpenDatabase() throws IOException {
    Random random = new Random();

    byte[] keyBytes = new byte[16];
    random.nextBytes(keyBytes);
    ByteArray key = new ByteArray(keyBytes);

    byte[] valueBytes = new byte[256];
    random.nextBytes(valueBytes);
    ByteArray value = new ByteArray(valueBytes);

    // Create database
    try (GuanacoStorageEngine guanacoStorageEngine = GuanacoStorageEngine.openDatabase(file)) {
      guanacoStorageEngine.put(key, value);
      assertEquals(value, guanacoStorageEngine.get(key));
    }

    // Reopen database
    try (GuanacoStorageEngine guanacoStorageEngine = GuanacoStorageEngine.openDatabase(file)) {
      assertEquals(value, guanacoStorageEngine.get(key));
    }
  }

  @Test
  public void testOpenDatabaseMultipleRecords() throws IOException {
    Random random = new Random();
    Map<ByteArray, ByteArray> map = new HashMap<>();

    // Create database
    try (GuanacoStorageEngine engine = GuanacoStorageEngine.createDatabase(file)) {
      for (int i = 0; i < 1000; ++i) {
        int operation = random.nextInt(3);
        if (operation == 0) {
          // Lets insert
          byte[] keyBytes = new byte[16];
          random.nextBytes(keyBytes);
          ByteArray key = new ByteArray(keyBytes);

          byte[] valueBytes = new byte[256];
          random.nextBytes(valueBytes);
          ByteArray value = new ByteArray(valueBytes);

          engine.put(key, value);
          map.put(key, value);
        } else if (operation == 1 && map.size() > 0) {
          // Lets modify
          byte[] valueBytes = new byte[256];
          random.nextBytes(valueBytes);
          ByteArray value = new ByteArray(valueBytes);

          int index = random.nextInt(map.size());
          ByteArray key = map.keySet().toArray(new ByteArray[0])[index];
          engine.put(key, value);
          map.put(key, value);
        } else if (operation == 2 && map.size() > 0) {
          // Lets delete
          int index = random.nextInt(map.size());
          ByteArray key = map.keySet().toArray(new ByteArray[0])[index];
          engine.delete(key);
          map.put(key, null);
        }
      }
    }

    // Reopen database
    try (GuanacoStorageEngine engine = GuanacoStorageEngine.openDatabase(file)) {
      for (ByteArray key : map.keySet()) {
        assertEquals(map.get(key), engine.get(key));
      }
    }
  }

}
