package org.apache.solr.handler.component;

import java.util.Comparator;

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

public class NullGoesLastComparator implements Comparator<Comparable> {  

  @Override
  public int compare(Comparable o1, Comparable o2) {
    boolean oneOfTheseIsNull = o1 == null || o2==null;
    if(oneOfTheseIsNull) {
      return handleSortWhenOneValueIsNull(o1,o2);
    }
    else
      return o1.compareTo(o2);
  }

  private int handleSortWhenOneValueIsNull(Comparable o1, Comparable o2) {    
      if(o1 == null) {
        if(o2 == null){
          return 0;
        }
        return 1;
      }
      else
        return -1;
    }
  }
  

