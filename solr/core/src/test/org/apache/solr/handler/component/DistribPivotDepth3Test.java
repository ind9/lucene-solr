package org.apache.solr.handler.component;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.common.params.FacetParams;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class DistribPivotDepth3Test extends BaseDistributedSearchTestCase {
  
  @Override
  public void doTest() throws Exception {
  this.del("*:*");
  index(id, 19, "place_s", "cardiff dublin", "company_t", "microsoft polecat", "people_t", "christian andrew monica","number_i",120);
  index(id, 20, "place_s", "dublin", "company_t", "polecat microsoft honda", "people_t", "monica john ryan bryan","number_i",87);
  index(id, 21, "place_s", "london la dublin", "company_t",
      "microsoft fujitsu honda polecat", "people_t", "jason chris johnnyb","number_i",66);
  index(id, 22, "place_s", "krakow london cardiff", "company_t",
      "polecat honda bbc", "people_t", "joe ryan","number_i",035);
  index(id, 23, "place_s", "london", "company_t", "", "people_t", "","number_i",2);
  index(id, 24, "place_s", "la", "company_t", "", "people_t", "christian chris jwoo","number_i",98);
  index(id, 25, "place_s", "", "company_t",
      "microsoft polecat honda fujitsu honda bbc", "people_t", "","number_i",67);
  index(id, 26, "place_s", "krakow", "company_t", "honda", "people_t", "joe christian jason","number_i",44);
  index(id, 27, "place_s", "krakow cardiff dublin london la", "company_t",
      "honda microsoft polecat bbc fujitsu", "people_t", "chris john christian","number_i",23);
  index(id, 28, "place_s", "cork", "company_t",
      "fujitsu rte");
  index(id,777);
  commit();
  
  handle.clear();
  handle.put("QTime", SKIPVAL);
  handle.put("timestamp", SKIPVAL);
  handle.put("maxScore", SKIPVAL);    
  
  this.query( "q", "*:*",
      "rows", "0",
      "facet","true",
      "facet.pivot","place_s,company_t,people_t,number_i"); //test default sort (count)

  this.query( "q", "*:*",
  "rows", "0",
  "facet","true",
  "facet.pivot","place_s,company_t,people_t,number_i",
  "facet.sort", "index"); //test sort by index order
  
  this.query( "q", "*:*",
      "rows", "0",
      "facet","true",
      "facet.pivot","place_s,company_t,people_t,number_i",
      FacetParams.FACET_MISSING, "true"); //test default sort (count)

  this.query( "q", "*:*",
  "rows", "0",
  "facet","true",
  "facet.pivot","place_s,company_t,people_t,number_i",
  FacetParams.FACET_MISSING, "true",
  FacetParams.FACET_LIMIT, "4",
  "facet.sort", "index"); //test sort by index order
  
  }
}
