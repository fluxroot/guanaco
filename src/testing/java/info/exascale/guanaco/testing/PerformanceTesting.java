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
package info.exascale.guanaco.testing;

import info.exascale.guanaco.ByteArray;
import info.exascale.guanaco.GuanacoStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PerformanceTesting {

  private static final Logger LOG = LoggerFactory.getLogger(PerformanceTesting.class);

  public static void main(String[] args) {
    List<ByteArray> keys = new ArrayList<>();

    GuanacoStorageEngine db = null;
    try {
      db = GuanacoStorageEngine.openDatabase();
      Random random = new Random();
      long insertOperations = 0;
      long modifyOperations = 0;
      long deleteOperations = 0;

      long startTime = System.currentTimeMillis();
      for (int i = 0; i < 1000000; ++i) {
        int operation = random.nextInt(3);
        if (operation == 0) {
          // Lets insert
          ++insertOperations;

          byte[] keyBytes = new byte[16];
          random.nextBytes(keyBytes);
          ByteArray key = new ByteArray(keyBytes);

          byte[] valueBytes = new byte[256];
          random.nextBytes(valueBytes);
          ByteArray value = new ByteArray(valueBytes);

          db.put(key, value);
          keys.add(key);
        } else if (operation == 1 && keys.size() > 0) {
          // Lets modify
          ++modifyOperations;

          byte[] valueBytes = new byte[256];
          random.nextBytes(valueBytes);
          ByteArray value = new ByteArray(valueBytes);

          int index = random.nextInt(keys.size());
          db.put(keys.get(index), value);
        } else if (operation == 2 && keys.size() > 0) {
          // Lets delete
          ++deleteOperations;

          int index = random.nextInt(keys.size());
          db.delete(keys.get(index));
          keys.remove(keys.get(index));
        }
      }
      db.close();
      db = null;
      long stopTime = System.currentTimeMillis();
      long duration = stopTime - startTime;

      LOG.info(String.format(
          "Duration: %02d:%02d:%02d.%03d%nInsert Operations: %d%nModify Operations: %d%nDelete Operations: %d%n",
          TimeUnit.MILLISECONDS.toHours(duration),
          TimeUnit.MILLISECONDS.toMinutes(duration) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(duration)),
          TimeUnit.MILLISECONDS.toSeconds(duration) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration)),
          duration - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(duration)),
          insertOperations,
          modifyOperations,
          deleteOperations
      ));
    } catch (IOException e) {
      LOG.error(e.getLocalizedMessage(), e);
    } finally {
      if (db != null) {
        try {
          db.close();
        } catch (IOException e) {
          // Do nothing
        }
      }
    }
  }

}
