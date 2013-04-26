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

package org.apache.solr.handler.component;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.PivotListEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

/**
 * TODO!
 *
 *
 * @since solr 1.3
 */
public class FacetComponent extends SearchComponent
{
  public static Logger log = LoggerFactory.getLogger(FacetComponent.class);

  public static final String COMPONENT_NAME = "facet";

  static final String PIVOT_KEY = "facet_pivot";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    if (rb.req.getParams().getBool(FacetParams.FACET,false)) {
      rb.setNeedDocSet( true );
      rb.doFacets = true;
    }
  }

  /**
   * Actually run the query
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    if (rb.doFacets) {
      SolrParams params = rb.req.getParams();
      SimpleFacets f = new SimpleFacets(rb.req,
          rb.getResults().docSet,
          params,
          rb );

      NamedList<Object> counts = f.getFacetCounts();
      String[] pivots = params.getParams( FacetParams.FACET_PIVOT );
      if (pivots != null && pivots.length > 0) {
        PivotFacetProcessor pivotProcessor = new PivotFacetProcessor(rb.req,
            rb.getResults().docSet, params, rb);
        NamedList v = pivotProcessor.process(pivots);
        if (v != null) {
          counts.add(PIVOT_KEY, v);
        }
      }
      rb.rsp.add( "facet_counts", counts );
    }
  }

  private static final String commandPrefix = "{!" + CommonParams.TERMS + "=$";

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (!rb.doFacets) {
      return ResponseBuilder.STAGE_DONE;
    }

    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      // overlap facet refinement requests (those shards that we need a count for
      // particular facet values from), where possible, with
      // the requests to get fields (because we know that is the
      // only other required phase).
      // We do this in distributedProcess so we can look at all of the
      // requests in the outgoing queue at once.



      for (int shardNum=0; shardNum<rb.shards.length; shardNum++) {
        List<String> distribFieldFacetRefinements = null;

        for (DistribFieldFacet dff : rb._facetInfo.facets.values()) {
          if (!dff.needRefinements) continue;
          List<String> refList = dff._toRefine[shardNum];
          if (refList == null || refList.size()==0) continue;

          String key = dff.getKey();  // reuse the same key that was used for the main facet
          String termsKey = key + "__terms";
          String termsVal = StrUtils.join(refList, ',');

          String facetCommand;
          // add terms into the original facet.field command
          // do it via parameter reference to avoid another layer of encoding.

          String termsKeyEncoded = QueryParsing.encodeLocalParamVal(termsKey);
          if (dff.localParams != null) {
            facetCommand = commandPrefix+termsKeyEncoded + " " + dff.facetStr.substring(2);
          } else {
            facetCommand = commandPrefix+termsKeyEncoded+'}'+dff.field;
          }

          if (distribFieldFacetRefinements == null) {
            distribFieldFacetRefinements = new ArrayList<String>();
          }

          distribFieldFacetRefinements.add(facetCommand);
          distribFieldFacetRefinements.add(termsKey);
          distribFieldFacetRefinements.add(termsVal);
        }

        List<String[]> pivotFacetRefinements = designPivotFacetRequestStrings(rb._facetInfo,shardNum);

        if (distribFieldFacetRefinements == null && pivotFacetRefinements == null) continue;


        String shard = rb.shards[shardNum];
        ShardRequest shardsRefineRequest = null;
        boolean newRequest = false;

        // try to find a request that is already going out to that shard.
        // If nshards becomes to great, we way want to move to hashing for better
        // scalability.
        for (ShardRequest sreq : rb.outgoing) {
          if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS)!=0
                  && sreq.shards != null
                  && sreq.shards.length==1
                  && sreq.shards[0].equals(shard))
          {
            shardsRefineRequest = sreq;
            break;
          }
        }

        if (shardsRefineRequest == null) {
          // we didn't find any other suitable requests going out to that shard, so
          // create one ourselves.
          newRequest = true;
          shardsRefineRequest = new ShardRequest();
          shardsRefineRequest.shards = new String[]{rb.shards[shardNum]};
          shardsRefineRequest.params = new ModifiableSolrParams(rb.req.getParams());
          // don't request any documents
          shardsRefineRequest.params.remove(CommonParams.START);
          shardsRefineRequest.params.set(CommonParams.ROWS,"0");
        }

        if (distribFieldFacetRefinements != null) {
          shardsRefineRequest.purpose |= ShardRequest.PURPOSE_REFINE_FACETS;
          shardsRefineRequest.params.set(FacetParams.FACET, "true");
          shardsRefineRequest.params.remove(FacetParams.FACET_FIELD);
          shardsRefineRequest.params.remove(FacetParams.FACET_QUERY);
          
          for (int i=0; i<distribFieldFacetRefinements.size();) {
            String facetCommand=distribFieldFacetRefinements.get(i++);
            String termsKey=distribFieldFacetRefinements.get(i++);
            String termsVal=distribFieldFacetRefinements.get(i++);
            
            shardsRefineRequest.params.add(FacetParams.FACET_FIELD, facetCommand);
            shardsRefineRequest.params.set(termsKey, termsVal);
          }
        }

        if (newRequest) {
          rb.addRequest(this, shardsRefineRequest);
        }
        
        // PivotFacetAdditions
        if(pivotFacetRefinements != null){
          if(newRequest){
            shardsRefineRequest.params.remove(FacetParams.FACET_PIVOT);
            shardsRefineRequest.params.remove(FacetParams.FACET_PIVOT_MINCOUNT);
          }

         enqueuePivotFacetShardRequests(pivotFacetRefinements,rb,shardNum);
        }
      }
    }

    return ResponseBuilder.STAGE_DONE;
  }
  

  int pivotRefinementCounter = 0;
  private List<String[]> designPivotFacetRequestStrings(FacetInfo fi, int shardNum){
    List<String[]> pivotFacetRefinements = null;
    if (fi.pivotFacetRefinementRequests.containsKey(shardNum)) {
      List<PivotFacetRefinement> refinementRequestsForShard = fi.pivotFacetRefinementRequests.get(shardNum);

      for (PivotFacetRefinement singleRequest : refinementRequestsForShard) {
        String fieldsKey = QueryParsing.encodeLocalParamVal(singleRequest.fieldsIdent+pivotRefinementCounter+"__terms");
        String command = commandPrefix + fieldsKey +"}" + singleRequest.fieldsIdent;
        String fields = singleRequest.printPath();
        if(pivotFacetRefinements==null){
          pivotFacetRefinements = new ArrayList<String[]>();
        }
        pivotFacetRefinements.add(new String[]{command,fieldsKey,fields});
        pivotRefinementCounter++;
      }
    }
    return pivotFacetRefinements;
  }

  private void enqueuePivotFacetShardRequests(List<String[]> pivotFacetRefinements,ResponseBuilder rb,int shardNum){
    
    ShardRequest shardsRefineRequestPivot = new ShardRequest();
    shardsRefineRequestPivot.shards = new String[] {rb.shards[shardNum]};
    shardsRefineRequestPivot.params = new ModifiableSolrParams(
        rb.req.getParams());
    // don't request any documents
    shardsRefineRequestPivot.params.remove(CommonParams.START);
    shardsRefineRequestPivot.params.set(CommonParams.ROWS, "0");
    
    
    shardsRefineRequestPivot.purpose |= ShardRequest.PURPOSE_REFINE_PIVOT_FACETS;
    shardsRefineRequestPivot.params.set(FacetParams.FACET, "true");
    shardsRefineRequestPivot.params.remove(FacetParams.FACET_PIVOT);
    shardsRefineRequestPivot.params.remove(FacetParams.FACET_LIMIT);
    shardsRefineRequestPivot.params.set(FacetParams.FACET_LIMIT, -1);
    shardsRefineRequestPivot.params.remove(FacetParams.FACET_MINCOUNT);
    shardsRefineRequestPivot.params.set(FacetParams.FACET_MINCOUNT, 1);
    shardsRefineRequestPivot.params.remove(FacetParams.FACET_PIVOT_MINCOUNT);
    shardsRefineRequestPivot.params.set(FacetParams.FACET_PIVOT_MINCOUNT, -1);
    
    for (String[] singleRequest : pivotFacetRefinements) {
      //0 Command 1 Key 2 Values
      shardsRefineRequestPivot.params.add(FacetParams.FACET_PIVOT, singleRequest[0]);
      shardsRefineRequestPivot.params.set(singleRequest[1],singleRequest[2]);
    }
    rb.addRequest(this,shardsRefineRequestPivot);
  }
  
  
  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doFacets) return;
    
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_FACETS;
      
      FacetInfo fi = rb._facetInfo;
      if (fi == null) {
        rb._facetInfo = fi = new FacetInfo();
        fi.parse(rb.req.getParams(), rb);
      }
      
      HashMap<String,String> fieldsToOverRequestOn = new HashMap<String,String>();
      
      sreq.params.remove(FacetParams.FACET_MINCOUNT);
      
      for (DistribFieldFacet dff : fi.facets.values()) {
			fieldsToOverRequestOn.put(dff.field, null);
        String paramStart = "f." + dff.field + '.';
        sreq.params.remove(paramStart + FacetParams.FACET_MINCOUNT);
        sreq.params.remove(paramStart + FacetParams.FACET_OFFSET);
        
        dff.initialLimit = dff.limit <= 0 ? dff.limit : dff.offset + dff.limit;
        
        if (dff.sort.equals(FacetParams.FACET_SORT_COUNT)) {
          if (dff.limit > 0) {
            // set the initial limit higher to increase accuracy
            dff.initialLimit = doOverRequestMath(dff.initialLimit);
            dff.initialMincount = 0;      // TODO: we could change this to 1, but would then need more refinement for small facet result sets?
          } else {
            // if limit==-1, then no need to artificially lower mincount to 0 if it's 1
            dff.initialMincount = Math.min(dff.minCount, 1);
          }
        } else {
          // we're sorting by index order.
          // if minCount==0, we should always be able to get accurate results w/o over-requesting or refining
          // if minCount==1, we should be able to get accurate results w/o over-requesting, but we'll need to refine
          // if minCount==n (>1), we can set the initialMincount to minCount/nShards, rounded up.
          // For example, we know that if minCount=10 and we have 3 shards, then at least one shard must have a count of 4 for the term
          // For the minCount>1 case, we can generate too short of a list (miss terms at the end of the list) unless limit==-1
          // For example: each shard could produce a list of top 10, but some of those could fail to make it into the combined list (i.e.
          //   we needed to go beyond the top 10 to generate the top 10 combined).  Overrequesting can help a little here, but not as
          //   much as when sorting by count.
          if (dff.minCount <= 1) {
            dff.initialMincount = dff.minCount;
          } else {
            dff.initialMincount = (int)Math.ceil((double)dff.minCount / rb.slices.length);
            // dff.initialMincount = 1;
          }
        }
        
        if (dff.initialMincount != 0) {
          sreq.params.set(paramStart + FacetParams.FACET_MINCOUNT, dff.initialMincount);
        }
        
        // Currently this is for testing only and allows overriding of the
        // facet.limit set to the shards
        dff.initialLimit = rb.req.getParams().getInt("facet.shard.limit", dff.initialLimit);
      }
      String[] commaSeparatedFieldsPivotingOn = sreq.params
          .getParams(FacetParams.FACET_PIVOT);
      
      if (commaSeparatedFieldsPivotingOn != null) {
        for (String commaSeparatedListOfFields : commaSeparatedFieldsPivotingOn) {
          for (String pivotField : StrUtils.splitSmart(commaSeparatedListOfFields, ',')) {
            fieldsToOverRequestOn.put(pivotField, null);
          }
        }
      }
      
      Iterator<String> fieldsIterator = fieldsToOverRequestOn.keySet()
          .iterator();
      
      while (fieldsIterator.hasNext()) {
        String fieldToOverRequest = fieldsIterator.next();
        DistribFieldFacet dff;
        int requestedLimit;
        if((dff = fi.facets.get(fieldToOverRequest)) !=null){//dff has the info, the params have been scrubbed
          requestedLimit = dff.initialLimit;
        } else { //dff did not have it, the params might contain it
          requestedLimit = sreq.params.getFieldInt(fieldToOverRequest,FacetParams.FACET_LIMIT, 100);
          sreq.params.remove("f." + fieldToOverRequest + "." + FacetParams.FACET_LIMIT);
        }
        
        
        if (requestedLimit > 0) {
          int modifiedLimit = requestedLimit;
          int offset = sreq.params.getFieldInt(fieldToOverRequest,
              FacetParams.FACET_OFFSET, 0);
          modifiedLimit += offset;
          String typeOfSorting = FacetParams.FACET_SORT_COUNT; // default
          // behavior
          // because
          // requestedLimit
          // > 0
          
          typeOfSorting = sreq.params.getFieldParam(fieldToOverRequest,
              FacetParams.FACET_SORT, typeOfSorting);
          if (typeOfSorting.equals(FacetParams.FACET_SORT_COUNT)) {
            modifiedLimit = doOverRequestMath(modifiedLimit);
          }
          
          modifiedLimit = rb.req.getParams().getInt("facet.shard.limit", modifiedLimit); // Currently this is for testing only and allows
          // overriding of the facet.limit sent to the shards
          
          sreq.params.set("f." + fieldToOverRequest + "."
              + FacetParams.FACET_LIMIT, modifiedLimit);
        }
      }
      sreq.params.remove(FacetParams.FACET_OFFSET);
      
      
    } else {
      // turn off faceting on other requests
      sreq.params.set(FacetParams.FACET, "false");
    }
  }
  
  private int doOverRequestMath(int someLimit) {
    return (int) (someLimit * 1.5) + 10;
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (!rb.doFacets) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FACETS)!=0) {
      countFacets(rb, sreq);
    } else if ((sreq.purpose & ShardRequest.PURPOSE_REFINE_FACETS)!=0) {
      refineFacets(rb, sreq);
    } else if ((sreq.purpose & ShardRequest.PURPOSE_REFINE_PIVOT_FACETS) != 0) {
      refinePivotFacets(rb, sreq);
  }
  }




  private void countFacets(ResponseBuilder rb, ShardRequest sreq) {
    FacetInfo fi = rb._facetInfo;

    for (ShardResponse srsp: sreq.responses) {
      int shardNum = rb.getShardNum(srsp.getShard());
      NamedList facet_counts = null;
      try {
        facet_counts = (NamedList)srsp.getSolrResponse().getResponse().get("facet_counts");
      }
      catch(Exception ex) {
        if(rb.req.getParams().getBool(ShardParams.SHARDS_TOLERANT, false)) {
          continue; // looks like a shard did not return anything
        }
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to read facet info for shard: "+srsp.getShard(), ex);
      }

      // handle facet queries
      NamedList facet_queries = (NamedList)facet_counts.get("facet_queries");
      if (facet_queries != null) {
        for (int i=0; i<facet_queries.size(); i++) {
          String returnedKey = facet_queries.getName(i);
          long count = ((Number)facet_queries.getVal(i)).longValue();
          QueryFacet qf = fi.queryFacets.get(returnedKey);
          qf.count += count;
        }
      }

      // step through each facet.field, adding results from this shard
      NamedList facet_fields = (NamedList)facet_counts.get("facet_fields");
    
      if (facet_fields != null) {
        for (DistribFieldFacet dff : fi.facets.values()) {
          dff.add(shardNum, (NamedList)facet_fields.get(dff.getKey()), dff.initialLimit);
        }
      }

      // Distributed facet_dates
      //
      // The implementation below uses the first encountered shard's 
      // facet_dates as the basis for subsequent shards' data to be merged.
      // (the "NOW" param should ensure consistency)
      @SuppressWarnings("unchecked")
      SimpleOrderedMap<SimpleOrderedMap<Object>> facet_dates = 
        (SimpleOrderedMap<SimpleOrderedMap<Object>>) 
        facet_counts.get("facet_dates");
      
      if (facet_dates != null) {

        // go through each facet_date
        for (Map.Entry<String,SimpleOrderedMap<Object>> entry : facet_dates) {
          final String field = entry.getKey();
          if (fi.dateFacets.get(field) == null) { 
            // first time we've seen this field, no merging
            fi.dateFacets.add(field, entry.getValue());

          } else { 
            // not the first time, merge current field

            SimpleOrderedMap<Object> shardFieldValues 
              = entry.getValue();
            SimpleOrderedMap<Object> existFieldValues 
              = fi.dateFacets.get(field);

            for (Map.Entry<String,Object> existPair : existFieldValues) {
              final String key = existPair.getKey();
              if (key.equals("gap") || 
                  key.equals("end") || 
                  key.equals("start")) {
                // we can skip these, must all be the same across shards
                continue; 
              }
              // can be null if inconsistencies in shards responses
              Integer newValue = (Integer) shardFieldValues.get(key);
              if  (null != newValue) {
                Integer oldValue = ((Integer) existPair.getValue());
                existPair.setValue(oldValue + newValue);
              }
            }
          }
        }
      }

      // Distributed facet_ranges
      //
      // The implementation below uses the first encountered shard's 
      // facet_ranges as the basis for subsequent shards' data to be merged.
      @SuppressWarnings("unchecked")
      SimpleOrderedMap<SimpleOrderedMap<Object>> facet_ranges = 
        (SimpleOrderedMap<SimpleOrderedMap<Object>>) 
        facet_counts.get("facet_ranges");
      
      if (facet_ranges != null) {

        // go through each facet_range
        for (Map.Entry<String,SimpleOrderedMap<Object>> entry : facet_ranges) {
          final String field = entry.getKey();
          if (fi.rangeFacets.get(field) == null) { 
            // first time we've seen this field, no merging
            fi.rangeFacets.add(field, entry.getValue());

          } else { 
            // not the first time, merge current field counts

            @SuppressWarnings("unchecked")
            NamedList<Integer> shardFieldValues 
              = (NamedList<Integer>) entry.getValue().get("counts");

            @SuppressWarnings("unchecked")
            NamedList<Integer> existFieldValues 
              = (NamedList<Integer>) fi.rangeFacets.get(field).get("counts");

            for (Map.Entry<String,Integer> existPair : existFieldValues) {
              final String key = existPair.getKey();
              // can be null if inconsistencies in shards responses
              Integer newValue = shardFieldValues.get(key);
              if  (null != newValue) {
                Integer oldValue = existPair.getValue();
                existPair.setValue(oldValue + newValue);
              }
            }
          }
        }
      }
      
      // Distributed facet_pivots
      //
      // The implementation below uses the first encountered shard's
      // facet_pivots as the basis for subsequent shards' data to be merged.
      SimpleOrderedMap<List<NamedList<Object>>> facet_pivot = (SimpleOrderedMap<List<NamedList<Object>>>) facet_counts.get("facet_pivot");

      if (facet_pivot != null) {
        if (fi.shardPivotFacets == null) {
          fi.shardPivotFacets = new LinkedHashMap<Integer,SimpleOrderedMap<Map<Comparable,NamedList<Object>>>>();
        }
        for (Map.Entry<String,List<NamedList<Object>>> pivot : facet_pivot) {
          final String pivotName = pivot.getKey();
          final Integer numberOfPivots = 1 + StringUtils.countMatches(pivotName, ",");

          // keep track per-shard for refinement purposes
          if (!fi.shardPivotFacets.containsKey(shardNum)) {
            fi.shardPivotFacets.put(shardNum,new SimpleOrderedMap<Map<Comparable,NamedList<Object>>>());
          }
          fi.shardPivotFacets.get(shardNum).add(pivot.getKey(), PivotFacetHelper.convertPivotsToMaps(pivot.getValue(), 1, numberOfPivots));
        }
      }

    } // end for-each-response-in-shard-request...

    
    // set pivot facets from map
    if (fi.shardPivotFacets.size() > 0) {
      //Combine from shards here
      WritePivotFacetCollections(rb,combinePivotFacetDataFromShards(fi));
    }    
    
    queuePivotRefinementRequests(rb);
    //
    // This code currently assumes that there will be only a single
    // request ((with responses from all shards) sent out to get facets...
    // otherwise we would need to wait until all facet responses were received.
    //

    for (DistribFieldFacet dff : fi.facets.values()) {
       // no need to check these facets for refinement
      if (dff.initialLimit <= 0 && dff.initialMincount <= 1) continue;

      // only other case where index-sort doesn't need refinement is if minCount==0
      if (dff.minCount <= 1 && dff.sort.equals(FacetParams.FACET_SORT_INDEX)) continue;

      @SuppressWarnings("unchecked") // generic array's are annoying
      List<String>[] tmp = (List<String>[]) new List[rb.shards.length];
      dff._toRefine = tmp;

      ShardFacetCount[] counts = dff.getCountSorted();
      int ntop = Math.min(counts.length, dff.limit >= 0 ? dff.offset + dff.limit : Integer.MAX_VALUE);
      long smallestCount = counts.length == 0 ? 0 : counts[ntop-1].count;

      for (int i=0; i<counts.length; i++) {
        ShardFacetCount sfc = counts[i];
        boolean needRefinement = false;

        if (i<ntop) {
          // automatically flag the top values for refinement
          // this should always be true for facet.sort=index
          needRefinement = true;
        } else {
          // this logic should only be invoked for facet.sort=index (for now)

          // calculate the maximum value that this term may have
          // and if it is >= smallestCount, then flag for refinement
          long maxCount = sfc.count;
          for (int shardNum=0; shardNum<rb.shards.length; shardNum++) {
            OpenBitSet obs = dff.counted[shardNum];
            if (obs!=null && !obs.get(sfc.termNum)) {  // obs can be null if a shard request failed
              // if missing from this shard, add the max it could be
              maxCount += dff.maxPossible(sfc,shardNum);
            }
          }
          if (maxCount >= smallestCount) {
            // TODO: on a tie, we could check the term values
            needRefinement = true;
          }
        }

        if (needRefinement) {
          // add a query for each shard missing the term that needs refinement
          for (int shardNum=0; shardNum<rb.shards.length; shardNum++) {
            OpenBitSet obs = dff.counted[shardNum];
            if(obs!=null && !obs.get(sfc.termNum) && dff.maxPossible(sfc,shardNum)>0) {
              dff.needRefinements = true;
              List<String> lst = dff._toRefine[shardNum];
              if (lst == null) {
                lst = dff._toRefine[shardNum] = new ArrayList<String>();
              }
              lst.add(sfc.name);
            }
          }
        }
      }
    }
  }

  private SimpleOrderedMap<Map<Comparable,NamedList<Object>>> combinePivotFacetDataFromShards(FacetInfo fi){

    SimpleOrderedMap<Map<Comparable,NamedList<Object>>> conglomeratedPivotFacetsMap = new SimpleOrderedMap<Map<Comparable,NamedList<Object>>>();

    for(Entry<Integer,SimpleOrderedMap<Map<Comparable,NamedList<Object>>>> shardResultMap : fi.shardPivotFacets.entrySet()){
      for(Entry<String,Map<Comparable,NamedList<Object>>> shardResultFromSpecificPivot : shardResultMap.getValue()){
        String specificPivotName = shardResultFromSpecificPivot.getKey();
        Map<Comparable,NamedList<Object>> shardResultFromSpecificPivotValue = PivotFacetHelper.cloneMapOfPivots(shardResultFromSpecificPivot.getValue());
        if(conglomeratedPivotFacetsMap.indexOf(specificPivotName, 0) == -1){//Doesn't Exist
          conglomeratedPivotFacetsMap.add(specificPivotName, new TreeMap<Comparable,NamedList<Object>>(new NullGoesLastComparator()));
        }
        Map<Comparable,NamedList<Object>> conglomMapOfPivotName = conglomeratedPivotFacetsMap.get(specificPivotName);
        int numberOfPivots = 1 + StringUtils.countMatches(specificPivotName, ",");
        mergePivotFacetMaps(conglomMapOfPivotName,shardResultFromSpecificPivotValue,1,numberOfPivots);
      }
    }
    return conglomeratedPivotFacetsMap;
  }

  private void mergePivotFacetMaps(Map<Comparable,NamedList<Object>> master, Map<Comparable,NamedList<Object>> toAdd, int currentPivotDepth, int pivotDepth){
    mergePivotFacetMapsWithOptions(master,toAdd,currentPivotDepth,pivotDepth,true);
  }

  private void mergePivotFacetShardMaps(Map<Comparable,NamedList<Object>> master, Map<Comparable,NamedList<Object>> toAdd, int currentPivotDepth, int pivotDepth){
    mergePivotFacetMapsWithOptions(master,toAdd,currentPivotDepth,pivotDepth,false);
  }

  private void mergePivotFacetMapsWithOptions(Map<Comparable,NamedList<Object>> master, Map<Comparable,NamedList<Object>> toAdd, int currentPivotDepth, int pivotDepth,boolean AddCounts){

    for(Entry<Comparable,NamedList<Object>> toAddValue : toAdd.entrySet()){

      NamedList<Object> toAddsPivotInfo = toAddValue.getValue();
      Comparable toAddsValue = PivotFacetHelper.getValue(toAddsPivotInfo);
      Integer toAddsValueCount = PivotFacetHelper.getCount(toAddsPivotInfo);
      NamedList<Object> masterAtShardValue = master.get(toAddsValue);
      if (masterAtShardValue == null) {
        // pivot value not found, add to existing values
        NamedList<Object> toAddsPivotInfoClone = toAddsPivotInfo.clone();//Shallow
        master.put(toAddsValue, toAddsPivotInfoClone);//Shallow top clone
      } else {
        if(AddCounts){
          int combinedCount = PivotFacetHelper.getCount(masterAtShardValue) + toAddsValueCount;
          PivotFacetHelper.setCount(combinedCount, masterAtShardValue);
        }
        if (currentPivotDepth < pivotDepth && toAddsPivotInfo.indexOf(PivotListEntry.MAPPEDPIVOT.getName(), 0)>-1) {

          Map<Comparable,NamedList<Object>> toAddPivotInfosPivot = PivotFacetHelper.cloneMapOfPivots(PivotFacetHelper.getMappedPivots(toAddsPivotInfo));

          if(masterAtShardValue.indexOf(PivotListEntry.MAPPEDPIVOT.getName(), 0)!=-1){
            PivotFacetHelper.setMappedPivots(PivotFacetHelper.cloneMapOfPivots(PivotFacetHelper.getMappedPivots(masterAtShardValue)), masterAtShardValue);//Clone the things the above shallow didn't get
          }
          Map<Comparable,NamedList<Object>> masterPivotAtShardValue = PivotFacetHelper.getMappedPivots(masterAtShardValue);
          if (toAddPivotInfosPivot instanceof Map) {
            if (masterPivotAtShardValue instanceof Map) {
              mergePivotFacetMapsWithOptions(masterPivotAtShardValue,toAddPivotInfosPivot, currentPivotDepth+1, pivotDepth,AddCounts);
            } else {// Assumption here that masterPivotAtShardValue is null or empty, replace with toAddInfosPivot
              masterAtShardValue.add(PivotListEntry.MAPPEDPIVOT.getName(), toAddPivotInfosPivot);
            }
          }
        }
      }
    }

  }

  private void WritePivotFacetCollections(ResponseBuilder rb, SimpleOrderedMap<Map<Comparable,NamedList<Object>>> pivotFacetsMap) {
    FacetInfo fi = rb._facetInfo;
    fi.combinedPivotFacets = PivotFacetHelper.convertPivotMapsToListAndSort(pivotFacetsMap, rb.req.getParams());
    fi.pivotFacets = PivotFacetHelper.trimExcessValuesBasedUponFacetLimit(PivotFacetHelper.clonePivotFacetList(fi.combinedPivotFacets), rb.req.getParams());
  }

  private void queuePivotRefinementRequests(ResponseBuilder rb) {
    FacetInfo fi = rb._facetInfo;

    for (Entry<String,List<NamedList<Object>>> pivotMapEntry : fi.combinedPivotFacets) {
      String pivotName = pivotMapEntry.getKey();
      Collection<NamedList<Object>> combinedPivots = pivotMapEntry.getValue();
      queuePivotRefinementRequestsHelper(rb, pivotName, combinedPivots,new LinkedHashMap<String,Comparable>());

    }
  }

  private void queuePivotRefinementRequestsHelper(ResponseBuilder rb,String pivotName, Collection<NamedList<Object>> combinedPivotFacets,LinkedHashMap<String,Comparable> fullPivotPath) {
    if (combinedPivotFacets.isEmpty()) return;

    NamedList<Object> firstCombinedResult = (NamedList<Object>) combinedPivotFacets.toArray()[0];
    String firstFieldName = PivotFacetHelper.getField(firstCombinedResult);

    SolrParams params = rb.req.getParams();
    boolean skipRefinementAtThisLevel = false;

    int facetLimit = params.getFieldInt(firstFieldName,
        FacetParams.FACET_LIMIT, 100);
    if (facetLimit < 0) {
      skipRefinementAtThisLevel = true;
    }

    int minCount = params.getFieldInt(firstFieldName,
        FacetParams.FACET_MINCOUNT, 0);
    if (facetLimit <= 0 && minCount == 0) {
      skipRefinementAtThisLevel = true;
    }

    String facetSort = params.getFieldParam(firstFieldName,
        FacetParams.FACET_SORT, facetLimit > 0 ? FacetParams.FACET_SORT_COUNT
            : FacetParams.FACET_SORT_INDEX);
    if (facetSort.equals(FacetParams.FACET_SORT_INDEX)) {
      skipRefinementAtThisLevel = true;
    }

    if (!skipRefinementAtThisLevel) {
      FacetInfo facetInfo = rb._facetInfo;
      int indexOfCountThreshold = combinedPivotFacets.size() >(facetLimit) ? facetLimit-1 : combinedPivotFacets.size()-1;
      int countThresholdAboveWhichToAutomaticallyRefine = PivotFacetHelper.getCount((NamedList<Object>) combinedPivotFacets.toArray()[indexOfCountThreshold]);
      int positionInResults = 0;

      for (NamedList<Object> singleCombinedEntry : combinedPivotFacets) {
        positionInResults++;
        processSinglePivotFacet(facetInfo, pivotName, singleCombinedEntry,
            fullPivotPath, facetLimit,countThresholdAboveWhichToAutomaticallyRefine, positionInResults);
      }
    }

    if (rb._facetInfo.pivotFacetRefinementRequests.size() == 0) {//We really want to check refinement for things we could have done above, not all when we hydra to deal with additional levels
      // If no refinement was queueued, Call self for every
      // pivotValues.item.pivot to take care of further pivot refinements below
      // this level, and update pivot path
      branchAdditionalRefinements(rb, pivotName, combinedPivotFacets,
          fullPivotPath);

    }

  }

  private void processSinglePivotFacet(FacetInfo facetInfo, String pivotName,NamedList<Object> singleCombinedEntry,
      LinkedHashMap<String,Comparable> fullPivotPath, int facetLimit,
      int countThresholdAboveWhichToAutomaticallyRefine, int positionInResults) {

    Comparable dataValue = PivotFacetHelper.getValue(singleCombinedEntry);
    String field = PivotFacetHelper.getField(singleCombinedEntry);
    LinkedHashMap<String,Comparable> currentFullPivotPathList = (LinkedHashMap<String,Comparable>) fullPivotPath.clone();
    currentFullPivotPathList.put(field, dataValue);

    if (positionInResults <= facetLimit) {// Within the top results, should
      // ensure we have responses from all
      // shards for this data
      processTopElement(facetInfo, pivotName, currentFullPivotPathList,
          facetLimit);
    } else {// Outside of the topThingsToReturnCount
      this.processNotTopElement(facetInfo, singleCombinedEntry, pivotName,
          currentFullPivotPathList,
          countThresholdAboveWhichToAutomaticallyRefine, dataValue, facetLimit);
    }
  }

  private void processTopElement(FacetInfo facetInfo, String pivotName, LinkedHashMap<String,Comparable> currentFullPivotPathList,int facetLimit) {

    for (Entry<Integer,SimpleOrderedMap<Map<Comparable,NamedList<Object>>>> shardsPivotFacets : facetInfo.shardPivotFacets.entrySet()) {
      int shardNum = shardsPivotFacets.getKey();
      Map<Comparable,NamedList<Object>> pivotResultsFromShard = shardsPivotFacets.getValue().get(pivotName);
      if (pivotResultsFromShard.size() > 0 && !PivotFacetHelper.doesSpecifiedLocationExist(pivotResultsFromShard, currentFullPivotPathList) && (PivotFacetHelper.getCountFromPath(pivotResultsFromShard,currentFullPivotPathList) >= facetLimit)) {
        refinePivotInformation(facetInfo, shardNum, new PivotFacetRefinement(pivotName, currentFullPivotPathList));
      }
    }
  }

  private void processNotTopElement(FacetInfo facetInfo,NamedList<Object> singleCombinedEntry, String pivotName,LinkedHashMap<String,Comparable> currentFullPivotPathList,
      int countThresholdAboveWhichToAutomaticallyRefine, Comparable value,int facetLimit) {

    int maxPossibleCountAfterRefinement = 0;

    for (Entry<Integer,SimpleOrderedMap<Map<Comparable,NamedList<Object>>>> shardPivotFacetsMap : facetInfo.shardPivotFacets.entrySet()) {
      int shardNum = shardPivotFacetsMap.getKey();
      Map<Comparable,NamedList<Object>> pivotResultsFromShard = shardPivotFacetsMap.getValue().get(pivotName);

      if (PivotFacetHelper.doesSpecifiedLocationExist(pivotResultsFromShard, currentFullPivotPathList)) {
        NamedList<Object> targetPivot = PivotFacetHelper.retrieveAtLocation( pivotResultsFromShard, currentFullPivotPathList);
        int targetCount = PivotFacetHelper.getCount(targetPivot);
        maxPossibleCountAfterRefinement += targetCount;
      } else {
        if(pivotResultsFromShard.size()>0){
          Integer minCount = Integer.MAX_VALUE;
          for (NamedList<Object> singlePivot : pivotResultsFromShard.values()) {
            int singlePivotCount = PivotFacetHelper.getCount(singlePivot);
            minCount = Math.min(minCount, singlePivotCount);
            if(minCount ==0) continue; //If we get to the bottom drop out
          }
          maxPossibleCountAfterRefinement += minCount;
        }
      }
    }
    if (maxPossibleCountAfterRefinement >= countThresholdAboveWhichToAutomaticallyRefine) {
      // Assume these could have been in the top 10, so pretend it was in top 10
      this.processTopElement(facetInfo, pivotName, currentFullPivotPathList,facetLimit);
    }
  }

  private void branchAdditionalRefinements(ResponseBuilder rb,String pivotName, Collection<NamedList<Object>> combinedPivotFacets,LinkedHashMap<String,Comparable> fullPivotPath) {
    for (NamedList<Object> singlePivotFacet : combinedPivotFacets) {
      if (PivotFacetHelper.getPivots(singlePivotFacet) != null) {
        LinkedHashMap<String,Comparable> futurePath = (LinkedHashMap<String,Comparable>) fullPivotPath.clone();
        futurePath.put(PivotFacetHelper.getField(singlePivotFacet),PivotFacetHelper.getValue(singlePivotFacet));
        this.queuePivotRefinementRequestsHelper(rb, pivotName,
            PivotFacetHelper.getPivots(singlePivotFacet), futurePath);
      }
    }
  }

  private void refinePivotInformation(FacetInfo fi, int shardNum, PivotFacetRefinement path) {
    if (!fi.pivotFacetRefinementRequests.containsKey(shardNum)) {
      fi.pivotFacetRefinementRequests.put(shardNum,new ArrayList<PivotFacetRefinement>());
    }
    fi.pivotFacetRefinementRequests.get(shardNum).add(path);
  }
  
  private void refineFacets(ResponseBuilder rb, ShardRequest sreq) {
    FacetInfo fi = rb._facetInfo;

    for (ShardResponse srsp: sreq.responses) {
      // int shardNum = rb.getShardNum(srsp.shard);
      NamedList facet_counts = (NamedList)srsp.getSolrResponse().getResponse().get("facet_counts");
      NamedList facet_fields = (NamedList)facet_counts.get("facet_fields");

      if (facet_fields == null) continue; // this can happen when there's an exception      

      for (int i=0; i<facet_fields.size(); i++) {
        String key = facet_fields.getName(i);
        DistribFieldFacet dff = fi.facets.get(key);
        if (dff == null) continue;

        NamedList shardCounts = (NamedList)facet_fields.getVal(i);

        for (int j=0; j<shardCounts.size(); j++) {
          String name = shardCounts.getName(j);
          long count = ((Number)shardCounts.getVal(j)).longValue();
          ShardFacetCount sfc = dff.counts.get(name);
          if (sfc == null) {
            // we got back a term we didn't ask for?
            log.error("Unexpected term returned for facet refining. key=" + key + " term='" + name + "'"
              + "\n\trequest params=" + sreq.params
              + "\n\ttoRefine=" + dff._toRefine
              + "\n\tresponse=" + shardCounts
            );
            continue;
          }
          sfc.count += count;
        }
      }
    }
  }
  
  private void refinePivotFacets(ResponseBuilder rb, ShardRequest sreq) {
    // This is after the shard has returned the refinement request
    FacetInfo fi = rb._facetInfo;
    LinkedHashMap<Integer,SimpleOrderedMap<Map<Comparable,NamedList<Object>>>> shardPivotFacets = fi.shardPivotFacets;
    for (ShardResponse srsp : sreq.responses) {
      int shardNum = rb.getShardNum(srsp.getShard());
      fi.pivotFacetRefinementRequests.remove(shardNum);
      SimpleOrderedMap<Map<Comparable,NamedList<Object>>> shardsPivot = shardPivotFacets.get(shardNum);

      NamedList facet_counts = (NamedList) srsp.getSolrResponse().getResponse().get("facet_counts");
      NamedList<List<NamedList<Object>>> response_facet_pivots = (NamedList<List<NamedList<Object>>>) facet_counts.get("facet_pivot");
      for (Entry<String,List<NamedList<Object>>> singleFacetPivotResponse : response_facet_pivots) {//This is each Facet.Pivot response
        Map<Comparable,NamedList<Object>> specificshardresult = shardsPivot.get(singleFacetPivotResponse.getKey());//Fields list is the key
        for (NamedList<Object> incomingPivotFacet : singleFacetPivotResponse.getValue()) {//Each field within this response (probably just one)
          int numberOfPivots = 1 + StringUtils.countMatches(singleFacetPivotResponse.getKey(), ",");//Fields list is the key
          Comparable pivotFacetsValue = PivotFacetHelper.getValue(incomingPivotFacet);
          if (specificshardresult.containsKey(pivotFacetsValue)) {
            NamedList<Object> theResultThatWillBeUpdated = specificshardresult.get(pivotFacetsValue);
            if(incomingPivotFacet.indexOf(PivotListEntry.PIVOT.getName(), 0)> 0){
              PivotFacetHelper.setMappedPivotsFromListPivots(incomingPivotFacet,numberOfPivots);
              PivotFacetHelper.remove(PivotListEntry.PIVOT, incomingPivotFacet);
            }
            mergePivotFacetShardMaps(PivotFacetHelper.getMappedPivots(theResultThatWillBeUpdated),PivotFacetHelper.getMappedPivots(incomingPivotFacet),1,numberOfPivots);
          } else {
            //Gotten from the shard response, is lists down below, not mapps, convert to merge properly with mapped shard results
            if(incomingPivotFacet.indexOf(PivotListEntry.PIVOT.getName(), 0)> 0){
              PivotFacetHelper.setMappedPivotsFromListPivots(incomingPivotFacet, numberOfPivots);
              PivotFacetHelper.remove(PivotListEntry.PIVOT, incomingPivotFacet);
            }
            specificshardresult.put(pivotFacetsValue, incomingPivotFacet);
          }
        }
      }
    }
    if(fi.pivotFacetRefinementRequests.size() == 0){ //All refinement requests have been received, we delete them when they come back
      WritePivotFacetCollections(rb,combinePivotFacetDataFromShards(fi));
      queuePivotRefinementRequests(rb);
      reQueuePivotFacetShardRequests(rb);
    }
  }

  private void reQueuePivotFacetShardRequests(ResponseBuilder rb){
    for (int shardNum = 0; shardNum < rb.shards.length; shardNum++) {
      List<String[]> pivotFacetRefinements = designPivotFacetRequestStrings(rb._facetInfo,shardNum);
      if(pivotFacetRefinements != null){
        enqueuePivotFacetShardRequests(designPivotFacetRequestStrings(rb._facetInfo,shardNum),rb,shardNum);
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!rb.doFacets || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
    // wait until STAGE_GET_FIELDS
    // so that "result" is already stored in the response (for aesthetics)


    FacetInfo fi = rb._facetInfo;

    NamedList<Object> facet_counts = new SimpleOrderedMap<Object>();

    NamedList<Number> facet_queries = new SimpleOrderedMap<Number>();
    facet_counts.add("facet_queries",facet_queries);
    for (QueryFacet qf : fi.queryFacets.values()) {
      facet_queries.add(qf.getKey(), num(qf.count));
    }

    NamedList<Object> facet_fields = new SimpleOrderedMap<Object>();
    facet_counts.add("facet_fields", facet_fields);

    for (DistribFieldFacet dff : fi.facets.values()) {
      NamedList<Object> fieldCounts = new NamedList<Object>(); // order is more important for facets
      facet_fields.add(dff.getKey(), fieldCounts);

      ShardFacetCount[] counts;
      boolean countSorted = dff.sort.equals(FacetParams.FACET_SORT_COUNT);
      if (countSorted) {
        counts = dff.countSorted;
        if (counts == null || dff.needRefinements) {
          counts = dff.getCountSorted();
        }
      } else if (dff.sort.equals(FacetParams.FACET_SORT_INDEX)) {
          counts = dff.getLexSorted();
      } else { // TODO: log error or throw exception?
          counts = dff.getLexSorted();
      }

      if (countSorted) {
        int end = dff.limit < 0 ? counts.length : Math.min(dff.offset + dff.limit, counts.length);
        for (int i=dff.offset; i<end; i++) {
          if (counts[i].count < dff.minCount) {
            break;
          }
          fieldCounts.add(counts[i].name, num(counts[i].count));
        }
      } else {
        int off = dff.offset;
        int lim = dff.limit >= 0 ? dff.limit : Integer.MAX_VALUE;

        // index order...
        for (int i=0; i<counts.length; i++) {
          long count = counts[i].count;
          if (count < dff.minCount) continue;
          if (off > 0) {
            off--;
            continue;
          }
          if (lim <= 0) {
            break;
          }
          lim--;
          fieldCounts.add(counts[i].name, num(count));
        }
      }

      if (dff.missing) {
        fieldCounts.add(null, num(dff.missingCount));
      }
    }

    facet_counts.add("facet_dates", fi.dateFacets);
    facet_counts.add("facet_ranges", fi.rangeFacets);
    
    facet_counts.add("facet_pivot", fi.pivotFacets);

    rb.rsp.add("facet_counts", facet_counts);

    rb._facetInfo = null;  // could be big, so release asap
  }


  // use <int> tags for smaller facet counts (better back compatibility)
  private Number num(long val) {
   if (val < Integer.MAX_VALUE) return (int)val;
   else return val;
  }
  private Number num(Long val) {
    if (val.longValue() < Integer.MAX_VALUE) return val.intValue();
    else return val;
  }


  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Handle Faceting";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class FacetInfo {

    public LinkedHashMap<String,QueryFacet> queryFacets;
    public LinkedHashMap<String,DistribFieldFacet> facets;
    public SimpleOrderedMap<SimpleOrderedMap<Object>> dateFacets
      = new SimpleOrderedMap<SimpleOrderedMap<Object>>();
    public SimpleOrderedMap<SimpleOrderedMap<Object>> rangeFacets
      = new SimpleOrderedMap<SimpleOrderedMap<Object>>();
    public SimpleOrderedMap<List<NamedList<Object>>> pivotFacets;
    
    public LinkedHashMap<Integer,SimpleOrderedMap<Map<Comparable,NamedList<Object>>>> shardPivotFacets
      = new LinkedHashMap<Integer,SimpleOrderedMap<Map<Comparable,NamedList<Object>>>>();
    public SimpleOrderedMap<List<NamedList<Object>>> combinedPivotFacets
      = new SimpleOrderedMap<List<NamedList<Object>>>();
    public Map<Integer,List<PivotFacetRefinement>> pivotFacetRefinementRequests
      = new HashMap<Integer,List<PivotFacetRefinement>>();

    void parse(SolrParams params, ResponseBuilder rb) {
      queryFacets = new LinkedHashMap<String,QueryFacet>();
      facets = new LinkedHashMap<String,DistribFieldFacet>();

      String[] facetQs = params.getParams(FacetParams.FACET_QUERY);
      if (facetQs != null) {
        for (String query : facetQs) {
          QueryFacet queryFacet = new QueryFacet(rb, query);
          queryFacets.put(queryFacet.getKey(), queryFacet);
        }
      }

      String[] facetFs = params.getParams(FacetParams.FACET_FIELD);
      if (facetFs != null) {

        // Indix.001 - Wildcard expansion in facet field
        List<String> indexedFieldsForFaceting = new ArrayList<String>();
        Collection<String> indexedFieldNames = rb.req.getSearcher().getFieldNames();

        for (int i = 0; i < facetFs.length; i++) {

          if (facetFs[i].contains("*")) {
            // Add the resolved fields
            String fieldRegex = facetFs[i].replaceAll("\\*", ".*");
            for (String indexedFieldName : indexedFieldNames) {
              if (indexedFieldName.matches(fieldRegex)) {
                indexedFieldsForFaceting.add(indexedFieldName);
              }
            }
          } else {
            // Add the original field
            indexedFieldsForFaceting.add(facetFs[i]);

          }

        }
        facetFs = indexedFieldsForFaceting.toArray(new String[]{});
        // Indix.001 - Wildcard expansion in facet field


        for (String field : facetFs) {
          DistribFieldFacet ff = new DistribFieldFacet(rb, field);
          facets.put(ff.getKey(), ff);
        }
      }
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class FacetBase {
    String facetType;  // facet.field, facet.query, etc (make enum?)
    String facetStr;   // original parameter value of facetStr
    String facetOn;    // the field or query, absent localParams if appropriate
    private String key; // label in the response for the result... "foo" for {!key=foo}myfield
    SolrParams localParams;  // any local params for the facet

    public FacetBase(ResponseBuilder rb, String facetType, String facetStr) {
      this.facetType = facetType;
      this.facetStr = facetStr;
      try {
        this.localParams = QueryParsing.getLocalParams(facetStr, rb.req.getParams());
      } catch (SyntaxError e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      this.facetOn = facetStr;
      this.key = facetStr;

      if (localParams != null) {
        // remove local params unless it's a query
        if (!facetType.equals(FacetParams.FACET_QUERY)) {
          facetOn = localParams.get(CommonParams.VALUE);
          key = facetOn;
        }

        key = localParams.get(CommonParams.OUTPUT_KEY, key);
      }
    }

    /** returns the key in the response that this facet will be under */
    public String getKey() { return key; }
    public String getType() { return facetType; }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class QueryFacet extends FacetBase {
    public long count;

    public QueryFacet(ResponseBuilder rb, String facetStr) {
      super(rb, FacetParams.FACET_QUERY, facetStr);
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class FieldFacet extends FacetBase {
    public String field;     // the field to facet on... "myfield" for {!key=foo}myfield
    public FieldType ftype;
    public int offset;
    public int limit;
    public int minCount;
    public String sort;
    public boolean missing;
    public String prefix;
    public long missingCount;

    public FieldFacet(ResponseBuilder rb, String facetStr) {
      super(rb, FacetParams.FACET_FIELD, facetStr);
      fillParams(rb, rb.req.getParams(), facetOn);
    }

    private void fillParams(ResponseBuilder rb, SolrParams params, String field) {
      this.field = field;
      this.ftype = rb.req.getSchema().getFieldTypeNoEx(this.field);
      this.offset = params.getFieldInt(field, FacetParams.FACET_OFFSET, 0);
      this.limit = params.getFieldInt(field, FacetParams.FACET_LIMIT, 100);
      Integer mincount = params.getFieldInt(field, FacetParams.FACET_MINCOUNT);
      if (mincount==null) {
        Boolean zeros = params.getFieldBool(field, FacetParams.FACET_ZEROS);
        // mincount = (zeros!=null && zeros) ? 0 : 1;
        mincount = (zeros!=null && !zeros) ? 1 : 0;
        // current default is to include zeros.
      }
      this.minCount = mincount;
      this.missing = params.getFieldBool(field, FacetParams.FACET_MISSING, false);
      // default to sorting by count if there is a limit.
      this.sort = params.getFieldParam(field, FacetParams.FACET_SORT, limit>0 ? FacetParams.FACET_SORT_COUNT : FacetParams.FACET_SORT_INDEX);
      if (this.sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        this.sort = FacetParams.FACET_SORT_COUNT;
      } else if (this.sort.equals(FacetParams.FACET_SORT_INDEX_LEGACY)) {
        this.sort = FacetParams.FACET_SORT_INDEX;
      }
      this.prefix = params.getFieldParam(field,FacetParams.FACET_PREFIX);
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class DistribFieldFacet extends FieldFacet {
    public List<String>[] _toRefine; // a List<String> of refinements needed, one for each shard.

    // SchemaField sf;    // currently unneeded

    // the max possible count for a term appearing on no list
    public long missingMaxPossible;
    // the max possible count for a missing term for each shard (indexed by shardNum)
    public long[] missingMax;
    public OpenBitSet[] counted; // a bitset for each shard, keeping track of which terms seen
    public HashMap<String,ShardFacetCount> counts = new HashMap<String,ShardFacetCount>(128);
    public int termNum;

    public int initialLimit;     // how many terms requested in first phase
    public int initialMincount;  // mincount param sent to each shard
    public boolean needRefinements;
    public ShardFacetCount[] countSorted;

    DistribFieldFacet(ResponseBuilder rb, String facetStr) {
      super(rb, facetStr);
      // sf = rb.req.getSchema().getField(field);
      missingMax = new long[rb.shards.length];
      counted = new OpenBitSet[rb.shards.length];
    }

    void add(int shardNum, NamedList shardCounts, int numRequested) {
      // shardCounts could be null if there was an exception
      int sz = shardCounts == null ? 0 : shardCounts.size();
      int numReceived = sz;

      OpenBitSet terms = new OpenBitSet(termNum+sz);

      long last = 0;
      for (int i=0; i<sz; i++) {
        String name = shardCounts.getName(i);
        long count = ((Number)shardCounts.getVal(i)).longValue();
        if (name == null) {
          missingCount += count;
          numReceived--;
        } else {
          ShardFacetCount sfc = counts.get(name);
          if (sfc == null) {
            sfc = new ShardFacetCount();
            sfc.name = name;
            sfc.indexed = ftype == null ? sfc.name : ftype.toInternal(sfc.name);
            sfc.termNum = termNum++;
            counts.put(name, sfc);
          }
          sfc.count += count;
          terms.fastSet(sfc.termNum);
          last = count;
        }
      }

      // the largest possible missing term is initialMincount if we received less
      // than the number requested.
      if (numRequested<0 || numRequested != 0 && numReceived < numRequested) {
        last = initialMincount;
      }

      missingMaxPossible += last;
      missingMax[shardNum] = last;
      counted[shardNum] = terms;
    }

    public ShardFacetCount[] getLexSorted() {
      ShardFacetCount[] arr = counts.values().toArray(new ShardFacetCount[counts.size()]);
      Arrays.sort(arr, new Comparator<ShardFacetCount>() {
        @Override
        public int compare(ShardFacetCount o1, ShardFacetCount o2) {
          return o1.indexed.compareTo(o2.indexed);
        }
      });
      countSorted = arr;
      return arr;
    }

    public ShardFacetCount[] getCountSorted() {
      ShardFacetCount[] arr = counts.values().toArray(new ShardFacetCount[counts.size()]);
      Arrays.sort(arr, new Comparator<ShardFacetCount>() {
        @Override
        public int compare(ShardFacetCount o1, ShardFacetCount o2) {
          if (o2.count < o1.count) return -1;
          else if (o1.count < o2.count) return 1;
          return o1.indexed.compareTo(o2.indexed);
        }
      });
      countSorted = arr;
      return arr;
    }

    // returns the max possible value this ShardFacetCount could have for this shard
    // (assumes the shard did not report a count for this value)
    long maxPossible(ShardFacetCount sfc, int shardNum) {
      return missingMax[shardNum];
      // TODO: could store the last term in the shard to tell if this term
      // comes before or after it.  If it comes before, we could subtract 1
    }
  }
  
  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class PivotFacetRefinement {
    public String fieldsIdent;
    public Map<String,Comparable> valuesPath;

    public PivotFacetRefinement(String fields, Map<String,Comparable> values) {
      this.fieldsIdent = fields;
      this.valuesPath = values;
    }

    public String toString() {
      return fieldsIdent + " | " + valuesPath.toString();
    }

    @SuppressWarnings("rawtypes")
    public String printPath(){
      return StrUtils.join(new ArrayList<Comparable>(this.valuesPath.values()), ',');
    }

    public String printFields(){
      List<String> fields = StrUtils.splitSmart(this.fieldsIdent, ',').subList(0, this.valuesPath.size());
      return StrUtils.join(fields, ','); 
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class ShardFacetCount {
    public String name;
    public String indexed;  // the indexed form of the name... used for comparisons.
    public long count;
    public int termNum;  // term number starting at 0 (used in bit arrays)

    @Override
    public String toString() {
      return "{term="+name+",termNum="+termNum+",count="+count+"}";
    }
  }
}
