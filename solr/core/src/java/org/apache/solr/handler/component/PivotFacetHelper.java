package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.PivotListEntry;


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

public class PivotFacetHelper {
  
  protected static Comparator<NamedList<Object>> namedListCountComparator = new PivotNamedListCountComparator();
  
  
  public static Object getFromPivotList(PivotListEntry entryToGet,
      NamedList<?> pivotList) {
    Object entry = pivotList.get(entryToGet.getName(), entryToGet.getIndex());
    if (entry == null) {
      entry = pivotList.get(entryToGet.getName());
    }
    return entry;
  }
    
  public static Map<Comparable,NamedList<Object>> convertPivotsToMaps(List<NamedList<Object>> pivots, int pivotsDone, int numberOfPivots) {
    
    Map<Comparable,NamedList<Object>> pivotMap = new TreeMap<Comparable,NamedList<Object>>(new NullGoesLastComparator());
    for (NamedList<Object> orgPivot : pivots) {
      NamedList<Object> pivot = orgPivot.clone();
      Comparable pivotValue = PivotFacetHelper.getValue(pivot);
      pivotMap.put(pivotValue, pivot);
      if (pivotsDone < numberOfPivots) {
        List<NamedList<Object>> pivPivot = PivotFacetHelper.getPivots(pivot);
        if (pivPivot != null) {
          PivotFacetHelper.remove(PivotListEntry.PIVOT, pivot);//Remove lists from maps
          PivotFacetHelper.setMappedPivots( convertPivotsToMaps(PivotFacetHelper.cloneListOfNamedList(pivPivot),pivotsDone + 1, numberOfPivots), pivot);
          int x = 1;
        }
      }
    }
    return pivotMap;
  }
  
  private static List<NamedList<Object>> cloneListOfNamedList(List<NamedList<Object>> listOfNamedList) {
    List<NamedList<Object>> retList = new LinkedList<NamedList<Object>>();
    for (NamedList<Object> namedList : listOfNamedList) {
      NamedList<Object> newNamedList = PivotFacetHelper.listNamedListCloner(namedList);
      retList.add(newNamedList);
    }
    return retList;
  }
  
  @SuppressWarnings("rawtypes")
  public static Map<Comparable,NamedList<Object>> cloneMapOfPivots(Map<Comparable,NamedList<Object>> map){
    Map<Comparable,NamedList<Object>> ret = new TreeMap<Comparable,NamedList<Object>>(new NullGoesLastComparator());
    
    for(Entry<Comparable,NamedList<Object>> thing : map.entrySet()){//clone the field,value,count and well come back to this for the future pivots
      ret.put(thing.getKey(), PivotFacetHelper.mapNamedListCloner(thing.getValue()));
    }
    return ret;
  }
  
  public static List<NamedList<Object>> trimList(int limit,List<NamedList<Object>> pivots){
    if (limit > 0
        && pivots.size() > limit) {
      pivots = new ArrayList<NamedList<Object>>(pivots.subList(0,limit));
    }
    return pivots;
  }
  
  public static void sortListOfNamedList(SolrParams params,List<NamedList<Object>> pivots){
    boolean sortByCount = true;
    if (pivots.size() > 0) {
       String fieldname = PivotFacetHelper.getField(pivots.get(0));
       sortByCount = sortByCountForThisField(fieldname, params);
    }
    
    if (sortByCount) {
      Collections.sort(pivots, namedListCountComparator);
    }
  }
  
  private static int getFacetLimitForThisField(String fieldname,
      SolrParams params) {
    return params.getFieldInt(fieldname, FacetParams.FACET_LIMIT, 100);
  }
  
  private static boolean sortByCountForThisField(String fieldname,
      SolrParams params) {
    String facetSort;
    String defaultFacetSort = FacetParams.FACET_SORT_INDEX;
    int facetLimit = getFacetLimitForThisField(fieldname, params);
    if (facetLimit > 0) {
      defaultFacetSort = FacetParams.FACET_SORT_COUNT;
    }
    String sortFromSolrParams = params.getFieldParam(fieldname,
        FacetParams.FACET_SORT);
    facetSort = sortFromSolrParams == null ? defaultFacetSort
        : sortFromSolrParams;
    return facetSort != null && facetSort.equals(FacetParams.FACET_SORT_COUNT);
  }
  
  public static SimpleOrderedMap<List<NamedList<Object>>> convertPivotMapsToListAndSort(SimpleOrderedMap<Map<Comparable,NamedList<Object>>> pivotFacetsMap,
                                                                                        SolrParams params) {

    SimpleOrderedMap<List<NamedList<Object>>> pivotsLists = new SimpleOrderedMap<List<NamedList<Object>>>();
    for (Entry<String,Map<Comparable,NamedList<Object>>> pivotMapEntry : pivotFacetsMap.clone()) {
      String pivotName = pivotMapEntry.getKey();
      Integer numberOfPivots = 1 + StringUtils.countMatches(pivotName, ",");
      int limit = params.getInt(FacetParams.FACET_LIMIT, 100);
      pivotsLists.add(pivotName,convertPivotMapToList(pivotMapEntry.getValue(), 0, numberOfPivots, params));
    }
    return pivotsLists;
  }
  
  private static void convertPivotEntryToListType(NamedList<Object> pivotEntry,int limit, int pivotsDone,int numberOfPivots, SolrParams params) {
   
    if (pivotsDone < numberOfPivots) {
      Map<Comparable,NamedList<Object>> subPivot = PivotFacetHelper.getMappedPivots(pivotEntry);
        if (subPivot !=null) {
          PivotFacetHelper.setPivot(convertPivotMapToList(subPivot, pivotsDone,numberOfPivots, params),pivotEntry);
          PivotFacetHelper.remove(PivotListEntry.MAPPEDPIVOT,pivotEntry); //Only Do this once refinement is done
        }
    }
  }
  
  private static List<NamedList<Object>> convertPivotMapToList(Map<Comparable,NamedList<Object>> map, int currentPivot,
      int numberOfPivots, SolrParams params) {
    
    List<NamedList<Object>> pivots = new ArrayList<NamedList<Object>>();
    currentPivot++;
    List<Object> fieldLimits = null;
    
    int limit = params.getInt(FacetParams.FACET_LIMIT, 100);
    for (Entry<Comparable,NamedList<Object>> pivot : map.entrySet()) {
      if (limit == 0 || fieldLimits == null || fieldLimits.contains(pivot.getKey())) {
        NamedList<Object> pivotClone = PivotFacetHelper.mapNamedListCloner(pivot.getValue().clone());
        pivots.add(pivotClone);
        convertPivotEntryToListType(pivotClone, limit, currentPivot, numberOfPivots, params);
      }
    }
    PivotFacetHelper.sortListOfNamedList(params,pivots);
    return pivots;
  }
  
  public static Map<Object,Integer> getFieldCountMap(
      Map<Integer,Map<Object,Integer>> fieldCounts, int pivotNumber) {
    Map<Object,Integer> fieldCountMap = fieldCounts.get(pivotNumber);
    if (fieldCountMap == null) {
      fieldCountMap = new HashMap<Object,Integer>();
      fieldCounts.put(pivotNumber, fieldCountMap);
    }
    return fieldCountMap;
  }
  
  public static void addFieldCounts(Object name, int count,
      Map<Object,Integer> thisFieldCountMap) {
    Integer existingFieldCount = thisFieldCountMap.get(name);
    if (existingFieldCount == null) {
      thisFieldCountMap.put(name, count);
    } else {
      thisFieldCountMap.put(name, existingFieldCount + count);
    }
  }
  
   public static SimpleOrderedMap<List<NamedList<Object>>> clonePivotFacetList(SimpleOrderedMap<List<NamedList<Object>>> org) {
     
     SimpleOrderedMap<List<NamedList<Object>>> ret = new SimpleOrderedMap<List<NamedList<Object>>>();
     
     for(Entry<String,List<NamedList<Object>>> pivots : org.clone()){
       ret.add(pivots.getKey(), PivotFacetHelper.cloneListOfNamedList(pivots.getValue()));
     }
     
    return ret; 
   }
   
   public static NamedList<Object> mapNamedListCloner(NamedList<Object> in){
     NamedList<Object> ret = in.clone();
     if(PivotFacetHelper.getMappedPivots(ret)!=null){
       PivotFacetHelper.setMappedPivots(PivotFacetHelper.cloneMapOfPivots(PivotFacetHelper.getMappedPivots(ret)),ret);
     }
     return ret;
   }
   
   public static NamedList<Object> listNamedListCloner(NamedList<Object> in){
     NamedList<Object> ret = in.clone();
     if(PivotFacetHelper.getPivots(ret)!=null){
       PivotFacetHelper.cloneListOfNamedList(PivotFacetHelper.getPivots(ret));
     }
     return ret;
   }
  
   public static SimpleOrderedMap<List<NamedList<Object>>> trimExcessValuesBasedUponFacetLimit(
		   SimpleOrderedMap<List<NamedList<Object>>> pivotFacets, SolrParams params) {

	   // enforce limit for each requested set of pivot facets
	   for (int i = 0; i < pivotFacets.size(); i++) {
		   pivotFacets.setVal(i,trimExcessHelper(pivotFacets.getVal(i), params));
	   }
	   return pivotFacets;
   }

   private static List<NamedList<Object>> trimExcessHelper(List<NamedList<Object>> val, SolrParams params) {
	   List<NamedList<Object>> newVal = new LinkedList<NamedList<Object>>();
	   if (val == null) return val;
	   NamedList<Object> nullValue = PivotFacetHelper.removeNullValue(val);



	   Iterator<NamedList<Object>> pivotValuesIterator = val.iterator();
	   if (pivotValuesIterator.hasNext()) {
		   NamedList<Object> pivotValue = pivotValuesIterator.next();
		   String currentFieldName = getFromPivotList(PivotListEntry.FIELD,
				   pivotValue).toString();
		   for(NamedList<Object> subVal : val){
			   if((PivotFacetHelper.getCount(subVal) >= params.getFieldInt(currentFieldName, FacetParams.FACET_PIVOT_MINCOUNT, 1))){
				   newVal.add(subVal);
			   }
		   }
		   if(newVal.size() >0){val = newVal;}//
		   int facetLimit = getFacetLimitForThisField(currentFieldName, params);
		   if (facetLimit > 0 && val.size() > facetLimit) {
			   val = val.subList(0, facetLimit);
		   }
	   }

	   if(nullValue != null ){
		   int minCount = params.getFieldInt(PivotFacetHelper.getField(nullValue), FacetParams.FACET_PIVOT_MINCOUNT, 1);
		   if(PivotFacetHelper.getCount(nullValue) >= minCount){//Only re-add if its count is over the facet's pivot mincount
			   val.add(nullValue);
		   }
	   }

	   for (int i = 0; i < val.size(); i++) {
		   val.set(i, handleOnePivot(val.get(i), params));
	   }

	   return val;

   }


  
  private static NamedList<Object> removeNullValue(List<NamedList<Object>> val){
    for(NamedList<Object> piv : val){
      if(PivotFacetHelper.getValue(piv) == null){
        val.remove(piv);
        return piv;
      }
    }
    return null;
  }
  
  private static NamedList<Object> handleOnePivot(NamedList<Object> pivot,SolrParams params) {
    
    int pivotIndex = pivot.indexOf(PivotListEntry.PIVOT.getName(), 0);
    if (pivotIndex > -1) {
      List<NamedList<Object>> furtherPivots = (List<NamedList<Object>>) pivot.getVal(pivotIndex);
      pivot.setVal(pivotIndex,trimExcessHelper(furtherPivots, params));
    }
    
    return pivot;
  }
  
  public static boolean doesSpecifiedLocationExist(Map<Comparable,NamedList<Object>> root, LinkedHashMap<String,Comparable> pivotPathToSeekTo) {
    try {
      isAnyValuePresentAtSpecifiedPath(root,pivotPathToSeekTo);
    } catch (SolrException e) {
      return false;
    }
    return true;
  }
  
  public static boolean isValuePresentAtSpecifiedLocation(Comparable value,Map<Comparable,NamedList<Object>> pivotRoot, Map<String,Comparable> pivotPathToSeekTo) {
    pivotRoot = seekToLocationsPivots(pivotRoot, pivotPathToSeekTo);//Seeks to the fresh pivots at the end of the list
    int i = findIndexOfPivotWithValue(value, pivotRoot);
    if (i < 0) return false;
    else return true;
  }
  
  private static boolean isAnyValuePresentAtSpecifiedPath(Map<Comparable,NamedList<Object>> pivotRoot, LinkedHashMap<String,Comparable> topPivotPathToSeekTo) {
    LinkedHashMap<String,Comparable> pivotPathToSeekTo = (LinkedHashMap<String,Comparable>)topPivotPathToSeekTo.clone();
    Comparable lastPartOfPath = (Comparable) pivotPathToSeekTo.values().toArray()[pivotPathToSeekTo.size()-1];//copy out
    pivotPathToSeekTo.remove(pivotPathToSeekTo.keySet().toArray()[pivotPathToSeekTo.size()-1]);//remove
    pivotRoot = seekToLocationsPivots(pivotRoot, pivotPathToSeekTo);//Seeks to the fresh pivots at the end of the list
    int i = findIndexOfPivotWithValue(lastPartOfPath, pivotRoot);
    if (i < 0 || i >= pivotRoot.size()){
      throw new SolrException(ErrorCode.SERVER_ERROR,"isAnyValuePresentAtSpecifiedPath");
    } else{ return true;}
  }
  
  public static int findIndexOfPivotWithValue(Comparable valueToFind,Map<Comparable,NamedList<Object>> pivots) {
    int i = 0;
    for (NamedList<Object> somePivot : pivots.values()) {
      Comparable pivotsValue = PivotFacetHelper.getValue(somePivot);
      if(pivotsValue == valueToFind) break;//compares two null values
      if (pivotsValue !=null && pivotsValue.equals(valueToFind)) break;//facet.missing=true can make pivotsValue equal null
      i++;
    }
    return i;
  }
  
  public static Map<Comparable,NamedList<Object>> seekToLocationsPivots(Map<Comparable,NamedList<Object>> root,Map<String,Comparable> pivotPathToSeekTo) {   
    for (Entry<String,Comparable> pathStep : pivotPathToSeekTo.entrySet()) {
      int i = findIndexOfPivotWithValue(pathStep.getValue(), root);
      if (i < 0 || i >= root.size()) throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "PivotFacetHelper was asked to seek within pivot tree but path within tree did not exist.");
      NamedList<Object> seekedLocation = (NamedList<Object>) root.values().toArray()[i];
      if(seekedLocation.indexOf(PivotListEntry.MAPPEDPIVOT.getName(), 0)==-1){
        throw new SolrException(ErrorCode.NOT_FOUND, "No further pivots exist on this shard");
      }
      root = PivotFacetHelper.getMappedPivots(seekedLocation);
    }
    return root;
  }
  
  public static NamedList<Object> retrieveAtLocation(Map<Comparable,NamedList<Object>> root,LinkedHashMap<String,Comparable> pivotPathToSeekTo) {
    LinkedHashMap<String,Comparable> pivotPath = (LinkedHashMap<String,Comparable>) pivotPathToSeekTo.clone();
    while(pivotPath.size() > 1){
      Comparable pathStep = (Comparable) pivotPath.values().toArray()[0];//Cheap pop
      String pathStepKey = (String) pivotPath.keySet().toArray()[0];//Cheap pop
      int i = findIndexOfPivotWithValue(pathStep, root);
      if (i < 0 || i >= root.size()) throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "StaticPivotFacetHelper was asked to seek within pivot tree but path within tree did not exist.");
      NamedList<Object> seekedLocation = (NamedList<Object>) root.values().toArray()[i];
      root = PivotFacetHelper.getMappedPivots(seekedLocation);
      if(root == null){
        throw new SolrException(ErrorCode.SERVER_ERROR, "PivotFacetHelper was asked to seek within pivot tree but path within tree did not exist. |"+pathStep);
      }
      pivotPath.remove(pathStepKey);
    }
    Comparable pathStep = (Comparable) pivotPath.values().toArray()[0];
    int i = findIndexOfPivotWithValue(pathStep, root);
    if (i < 0 || i >= root.size()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "PivotFacetHelper was asked to seek within pivot tree but path within tree did not exist. |"+pathStep);
      //return new NamedList<Object>();
    }

    return (NamedList<Object>) root.values().toArray()[i];
  }
  
  public static int getCountFromPath(Map<Comparable,NamedList<Object>> root, LinkedHashMap<String,Comparable> path){
    if(path.size()==1){
      return root.values().size();
    }else{
    LinkedHashMap<String,Comparable> minusPath = (LinkedHashMap<String,Comparable>) path.clone();
    Comparable pathStep = (Comparable) minusPath.values().toArray()[minusPath.size()-1];//Cheap getLast
    String pathStepKey = (String) minusPath.keySet().toArray()[minusPath.size()-1];//Cheap getLast
    minusPath.remove(pathStepKey);
    
    NamedList<Object> temp;
    
    try {
      temp = PivotFacetHelper.retrieveAtLocation(root, minusPath);
    } catch (SolrException e) {
      return 0;
    }
    return temp.indexOf(PivotListEntry.MAPPEDPIVOT.getName(), 0)> -1 ? PivotFacetHelper.getMappedPivots(temp).size() : 0 ;
    }
    
  }
  
  public static void setValue(Comparable value, NamedList<Object> pivotList) {
    PivotFacetHelper.set(value,PivotListEntry.VALUE,pivotList);
  }
  
  public static void setField(String field, NamedList<Object> pivotList) {
    PivotFacetHelper.set(field,PivotListEntry.FIELD,pivotList);
  }
  
  public static void setCount(int count, NamedList<Object> pivotList) {
    PivotFacetHelper.set(count,PivotListEntry.COUNT,pivotList);
  }
  
  public static void setPivot(List<NamedList<Object>> pivots,NamedList<Object> pivotList) {
   PivotFacetHelper.set(pivots,PivotListEntry.PIVOT,pivotList);
  }
  
  private static void set(Object thing,PivotListEntry type, NamedList<Object> pivotList){
    int typeLocation = pivotList.indexOf(type.getName(), 0);
    
    if (typeLocation > -1) {//Exists
     pivotList.setVal(typeLocation, thing);
    } else{
      pivotList.add(type.getName(), thing);
    }
  }
  
  public static void setMappedPivots(Map<Comparable,NamedList<Object>> mappedPivots,NamedList<Object> pivotList) {
    PivotFacetHelper.set(mappedPivots,PivotListEntry.MAPPEDPIVOT,pivotList);
  }
  
  public static Comparable getValue(NamedList<Object> pivotList) {
    return (Comparable) PivotFacetHelper.retrieve(PivotListEntry.VALUE,
        pivotList);
  }
  
  public static String getField(NamedList<Object> pivotList) {
    return (String) PivotFacetHelper.retrieve(PivotListEntry.FIELD, pivotList);
  }
  
  public static Integer getCount(NamedList<Object> pivotList) {
    return (Integer) PivotFacetHelper.retrieve(PivotListEntry.COUNT, pivotList);
  }
  
  public static List<NamedList<Object>> getPivots(NamedList<Object> pivotList) {
    int pivotIdx = pivotList.indexOf(PivotListEntry.PIVOT.getName(), 0);
    if (pivotIdx > -1) {
      return (List<NamedList<Object>>) pivotList.getVal(pivotIdx);
    }
    return null;// or an emtpy list?
  }
  
  public static void remove(PivotListEntry type,NamedList<Object> pivotList){
    pivotList.remove(type.getName());
  }
  
  public static Map<Comparable,NamedList<Object>> getMappedPivots(NamedList<Object> pivotList) {
    int pivotIdx = pivotList.indexOf(PivotListEntry.MAPPEDPIVOT.getName(), 0);
    if (pivotIdx > -1) {
      return (Map<Comparable,NamedList<Object>>) pivotList.getVal(pivotIdx);
    }
    return null;// or an emtpy list?
  }
  
  public static boolean furtherPivotsExist(NamedList<Object> pivotList) {
    return PivotFacetHelper.getPivots(pivotList) != null;
  }
  
  private static Object retrieve(PivotListEntry entryToGet, NamedList<Object> pivotList) {
    Object entry = pivotList.get(entryToGet.getName(), entryToGet.getIndex());
    if (entry == null) {
      entry = pivotList.get(entryToGet.getName());
    }
    return entry;
  }
  
  public static void setMappedPivotsFromListPivots(NamedList<Object> pivotList,int numberOfPivots){
    PivotFacetHelper.setMappedPivots(PivotFacetHelper.convertPivotsToMaps(PivotFacetHelper.getPivots(pivotList), 1, numberOfPivots),pivotList);
  }
  
  public static class PivotLimitInfo {  
    public int limit = 0;
  
  }
  
  private static class InternalPivotLimitInfo {
    
    public int limit = 0;
    
    private InternalPivotLimitInfo() {}
    
    private InternalPivotLimitInfo(PivotLimitInfo pivotLimitInfo,
        String pivotName) {
      this.limit = pivotLimitInfo.limit;
    }
    
    private InternalPivotLimitInfo(InternalPivotLimitInfo pivotLimitInfo) {
      this.limit = pivotLimitInfo.limit;
    }
  }
  
}
