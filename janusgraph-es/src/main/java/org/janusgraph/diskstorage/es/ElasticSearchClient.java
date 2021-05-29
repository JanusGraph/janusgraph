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

    void bulkRequest(List<ElasticSearchMutation> requests, String ingestPipeline) throws IOException;

    long countTotal(String indexName, Map<String,Object> requestData) throws IOException;

    ElasticSearchResponse search(String indexName, Map<String,Object> request, boolean useScroll) throws IOException;

    ElasticSearchResponse search(String scrollId) throws IOException;

    void deleteScroll(String scrollId) throws IOException;

    void addAlias(String alias, String index) throws IOException;

}
