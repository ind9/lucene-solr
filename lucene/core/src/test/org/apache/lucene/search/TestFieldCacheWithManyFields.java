package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFieldCacheWithManyFields extends LuceneTestCase {
  private static AtomicReader reader;
  private static int NUM_DOCS;
  private static int NUM_FIELDS;
  private static int MAX_FIELD_SUFFIX;
  private static Directory directory;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    
    NUM_DOCS = atLeast(200);
    NUM_FIELDS = atLeast(10);
    MAX_FIELD_SUFFIX = atLeast(1000);
    
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)) 
        .setMergePolicy(newLogMergePolicy()));
    long theLong = Long.MAX_VALUE;
    if (VERBOSE) {
      System.out.println("TEST: setUp");
    }
    for (int i = 1; i <= NUM_DOCS; i++) {
      Document doc = new Document();
      int fs = random.nextInt(MAX_FIELD_SUFFIX);
      for (int j = 0; j < NUM_FIELDS; j++) {
        doc.add(newField("theLong_" + fs, String.valueOf(theLong--), StringField.TYPE_UNSTORED));
      }
      writer.addDocument(doc);
    }
    IndexReader r = writer.getReader();
    reader = SlowCompositeReaderWrapper.wrap(r);
    writer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }
  
  public void testWithoutLimitingCaches() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purge(reader);
    exerciseCache(cache);
    assertTrue(cache.getCacheEntries().length > 1000);
    
  }

  public void testWithLimitedCaches() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    
    System.setProperty("lucene.fieldCache.maxFields", "50");
    cache.purge(reader);
    exerciseCache(cache);
    assertTrue("cache entries length is " + cache.getCacheEntries().length , cache.getCacheEntries().length < 200);
    
    System.getProperties().remove("lucene.fieldCache.maxFields");
  }
  
  private void exerciseCache(FieldCache cache) throws IOException {
    for (int i = 1; i <= 2000; i++) {
      int fs = random.nextInt(MAX_FIELD_SUFFIX);
      long[] longs = cache.getLongs(reader, "theLong_" + fs, random.nextBoolean());
    }
  }

}
