// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.es;

import org.janusgraph.diskstorage.es.mapping.IndexMapping;
import org.janusgraph.diskstorage.es.script.ESScriptResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ElasticSearchClient extends Closeable {

    ElasticMajorVersion getMajorVersion();

    /**
     * Returns whether requests use mapping types: those of Elasticsearch 6, and of Elasticsearch 7 with
     * {@code use-mapping-for-es7}. The default only knows the major version, so it only covers Elasticsearch 6.
     */
    default boolean usesMappingTypes() {
        return getMajorVersion().getValue() < 7;
    }

    void clusterHealthRequest(String timeout) throws IOException;

    boolean indexExists(String indexName) throws IOException;

    boolean isIndex(String indexName);

    boolean isAlias(String aliasName);

    void createStoredScript(String scriptName, Map<String,Object> script) throws IOException;

    ESScriptResponse getStoredScript(String scriptName) throws IOException;

    void createIndex(String indexName, Map<String,Object> settings) throws IOException;

    void updateIndexSettings(String indexName, Map<String,Object> settings) throws IOException;

    void updateClusterSettings(Map<String,Object> settings) throws IOException;

    Map getIndexSettings(String indexName) throws IOException;

    void createMapping(String indexName, String typeName, Map<String,Object> mapping) throws IOException;

    IndexMapping getMapping(String indexName, String typeName) throws IOException;

    void deleteIndex(String indexName) throws IOException;

    /**
     * Deletes the Elasticsearch index which backs a store, if it exists.
     *
     * @param indexStoreName the Elasticsearch index name of the store, already derived by the caller from the
     *                       JanusGraph store name, so that the mapping between the two exists in exactly one place.
     *                       It is used verbatim; in particular it is not lowercased here.
     */
    void clearStore(String indexStoreName) throws IOException;

    void bulkRequest(List<ElasticSearchMutation> requests, String ingestPipeline) throws IOException;

    long countTotal(String indexName, Map<String,Object> requestData) throws IOException;

    /**
     * Counts the documents which match the query, with a bound when {@code atMost} is positive: the count is then
     * exact below the bound and at least the bound above it, which suits a caller that wants no more than the bound,
     * as it clamps the count to its limit anyway. This default counts everything, which satisfies that.
     */
    default long countTotal(String indexName, Map<String,Object> requestData, int atMost) throws IOException {
        return countTotal(indexName, requestData);
    }

    Number min(String indexName, Map<String,Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException;

    Number max(String indexName, Map<String,Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException;

    double avg(String indexName, Map<String,Object> requestData, String fieldName) throws IOException;

    Number sum(String indexName, Map<String,Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException;

    ElasticSearchResponse search(String indexName, Map<String,Object> request, boolean useScroll) throws IOException;

    ElasticSearchResponse search(String scrollId) throws IOException;

    /**
     * Releases a scroll context which is no longer read. It is a courtesy to the cluster rather than part of the
     * search: the context expires after the scroll keep-alive on its own, so an implementation may return before
     * the cluster has released it, and the caller doesn't fail its search over a release which failed.
     */
    void deleteScroll(String scrollId) throws IOException;

    void addAlias(String alias, String index) throws IOException;

}
