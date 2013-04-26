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
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @since solr 4.0
 */
public class PivotFacetProcessor extends SimpleFacets
{
  protected int minMatch;
  
  public PivotFacetProcessor(SolrQueryRequest req, DocSet docs, SolrParams params, ResponseBuilder rb) {
    super(req, docs, params, rb);
    minMatch = params.getInt( FacetParams.FACET_PIVOT_MINCOUNT, 1 );
  }
  
  public SimpleOrderedMap<List<NamedList<Object>>> process(String[] pivots) throws IOException {
    if (!rb.doFacets || pivots == null) 
      return null;
    
    SimpleOrderedMap<List<NamedList<Object>>> pivotResponse = new SimpleOrderedMap<List<NamedList<Object>>>();
    for (String pivotList : pivots) {
      //ex: pivot == "features,cat" or even "{!ex=mytag}features,cat"
      try {
        this.parseParams(FacetParams.FACET_PIVOT, pivotList);
      } catch (SyntaxError e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
      pivotList = facetValue;//facetValue potentially modified from parseParams()
      List<String> fields = StrUtils.splitSmart(pivotList, ",", true);
      
      
      if( fields.size() < 1 ) {
        throw new SolrException( ErrorCode.BAD_REQUEST,
            "Pivot Facet needs at least one field: "+pivotList );
      }
      
      String field = fields.get(0);
      
      Deque<String> fnames = new LinkedList<String>();
      for( int i=fields.size()-1; i>1; i-- ) {
        fnames.push( fields.get(i) );
      }
      
      NamedList<Integer> superFacets = this.getTermCountsForPivots(field);
      
      //REFINEMENT
      String fieldValueList = localParams == null ? null : localParams.get(CommonParams.TERMS);
      Deque<String> vnames = new LinkedList<String>();
      if(fieldValueList != null ){//We have terms params
        List<String> refinementValuesByField  = StrUtils.splitSmart(fieldValueList, ",", true);//All values, split by the field they should go to
        for( int i=refinementValuesByField.size()-1; i>0; i-- ) {
          vnames.push(refinementValuesByField.get(i));//Only for [1] and on
        }
        String firstFieldsValues = "";
        if(refinementValuesByField.size()>0){firstFieldsValues = refinementValuesByField.get(0);}//First field's values
        
        superFacets = hijackFacets(superFacets,firstFieldsValues);
      }
      //END REFINEMENT
      
      
      //super.key usually == pivot unless local-param 'key' used
      if(fields.size() > 1) {
        String subField = fields.get(1);
        pivotResponse.add(key,
            doPivots(superFacets, field, subField, fnames, vnames, docs));
      }
      else {
        pivotResponse.add(key,doPivots(superFacets,field,null, fnames, vnames, docs));
      }
      
    }
    return pivotResponse;
  }
  
  /**
   * Recursive function to do all the pivots
   */
  protected List<NamedList<Object>> doPivots(NamedList<Integer> superFacets,
      String field, String subField, Deque<String> fnames,Deque<String> vnames,DocSet docs) throws IOException {
    
    SolrIndexSearcher searcher = rb.req.getSearcher();
    // TODO: optimize to avoid converting to an external string and then having to convert back to internal below
    SchemaField sfield = searcher.getSchema().getField(field);
    FieldType ftype = sfield.getType();
    
    String nextField = fnames.poll();
    
    List<NamedList<Object>> values = new ArrayList<NamedList<Object>>( superFacets.size() );
    for (Map.Entry<String, Integer> kv : superFacets) {
      // Only sub-facet if parent facet has positive count - still may not be any values for the sub-field though
      if (kv.getValue() >= minMatch) {  
        // may be null when using facet.missing
        final String fieldValue = kv.getKey();
        
        // don't reuse the same BytesRef each time since we will be 
        // constructing Term objects used in TermQueries that may be cached.
        BytesRef termval = null;
        
        SimpleOrderedMap<Object> pivot = new SimpleOrderedMap<Object>();
        pivot.add( "field", field );
        if (null == fieldValue) {
          pivot.add( "value", null );
        } else {
          termval = new BytesRef();
          ftype.readableToIndexed(fieldValue, termval);
          pivot.add( "value", ftype.toObject(sfield, termval) );
        }
        pivot.add( "count", kv.getValue() );
        
        
        DocSet subset = null;
        if ( null == termval ) {
          DocSet hasVal = searcher.getDocSet(new TermRangeQuery(field, null, null, false, false));
          subset = docs.andNot(hasVal);
        } else {
          Query query = new TermQuery(new Term(field, termval));
          subset = searcher.getDocSet(query, docs);
        }
        super.docs = subset;        
        if( subField != null )  {
          NamedList<Integer> nl = this.getTermCountsForPivots(subField);
          if(!vnames.isEmpty()){ nl = hijackFacets(nl,vnames.pop());}
          
          if (nl.size() >= minMatch) {
            pivot.add( "pivot", doPivots( nl, subField, nextField, fnames, vnames, subset) );
          }
        }
        values.add( pivot );
      }
      
    }
    // put the field back on the list
    fnames.push( nextField );
    return values;
  }
  
  private NamedList<Integer> hijackFacets(NamedList<Integer> superFacets, String firstFieldsValues){
    if(firstFieldsValues.compareTo("null")==0){firstFieldsValues=null;}
    NamedList<Integer> mostSuperFacets = new NamedList<Integer>();
      if(superFacets.get(firstFieldsValues)!=null){
        mostSuperFacets.add(firstFieldsValues, superFacets.get(firstFieldsValues));
      }else{
        mostSuperFacets.add(firstFieldsValues, 0);
      }
    if(mostSuperFacets.size() > 0){
      return mostSuperFacets;
    } else {
      return superFacets;
    }
  }
}
