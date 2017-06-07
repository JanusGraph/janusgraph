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

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_DOC_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_INLINE_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_LANG_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_SCRIPT_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_UPSERT_KEY;

import org.janusgraph.diskstorage.indexing.RawQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportElasticSearchClient implements ElasticSearchClient {

    private static final Logger log = LoggerFactory.getLogger(TransportElasticSearchClient.class);

    private Client client;

    private boolean bulkRefresh;

    public TransportElasticSearchClient(Client client) {
        this.client = client;
    }

    @Override
    public void clusterHealthRequest(String timeout) throws IOException {
        client.admin().cluster().prepareHealth().setTimeout(timeout).setWaitForYellowStatus().execute().actionGet();
    }

    @Override
    public boolean indexExists(String indexName) throws IOException {
        IndicesExistsResponse response = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
        return response.isExists();
    }

    @Override
    public void createIndex(String indexName, Settings settings) throws IOException {
        CreateIndexResponse create = client.admin().indices().prepareCreate(indexName)
            .setSettings(settings).execute().actionGet();
    }

    @Override
    public Map getIndexSettings(String indexName) throws IOException {
        GetSettingsResponse response = client.admin().indices().getSettings(new GetSettingsRequest().indices(indexName)).actionGet();
        return response.getIndexToSettings().get(indexName).getAsMap().entrySet().stream()
            .collect(Collectors.toMap(e->e.getKey().replace("index.",""), Map.Entry::getValue));
    }

    @Override
    public void createMapping(String indexName, String typeName, XContentBuilder mapping) throws IOException {
        client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(mapping).execute().actionGet();
    }

    @Override
    public Map getMapping(String indexName, String typeName) throws IOException {
        GetMappingsResponse response = client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName.toLowerCase()).types(typeName)).actionGet();
        return (Map)response.getMappings().get(indexName.toLowerCase()).get(typeName).getSourceAsMap().get("properties");
    }

    @Override
    public void deleteIndex(String indexName) throws IOException {
        try {
            client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
            // We wait for one second to let ES delete the river
            Thread.sleep(1000);
        } catch (IndexNotFoundException e) {
            // Index does not exist... Fine
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void bulkRequest(List<ElasticSearchMutation> requests) throws IOException {
        BulkRequestBuilder brb = client.prepareBulk();
        requests.stream().forEach(request -> {
            String indexName = request.getIndex();
            String type = request.getType();
            String id = request.getId();
            switch (request.getRequestType()) {
                case DELETE: {
                    brb.add(new DeleteRequest(indexName, type, id));
                    break;
                } case INDEX: {
                    brb.add(new IndexRequest(indexName, type, id).source(request.getSource()));
                    break;
                } case UPDATE: {
                    UpdateRequestBuilder update = client.prepareUpdate(indexName, type, id);
                    if (request.getSource().containsKey(ES_SCRIPT_KEY)) {
                        Map<String,String> script = ((Map<String, String>) request.getSource().get(ES_SCRIPT_KEY));
                        String inline = script.get(ES_INLINE_KEY);
                        String lang = script.get(ES_LANG_KEY);
                        update.setScript(new Script(inline, ScriptService.ScriptType.INLINE, lang, null));
                    }
                    if (request.getSource().containsKey(ES_DOC_KEY)) {
                        update.setDoc((Map) request.getSource().get(ES_DOC_KEY));
                    }
                    if (request.getSource().containsKey(ES_UPSERT_KEY)) {
                        update.setUpsert((Map) request.getSource().get(ES_UPSERT_KEY));
                    }
                    brb.add(update);
                    break;
                } default:
                    throw new IllegalArgumentException("Unsupported request type: " + request.getRequestType());
            }
        });

        if (!requests.isEmpty()) {
            if (bulkRefresh) {
                brb.setRefresh(true);
            }
            BulkResponse bulkItemResponses = brb.execute().actionGet();
            if (bulkItemResponses.hasFailures()) {
                boolean actualFailure = false;
                for(BulkItemResponse response : bulkItemResponses.getItems()) {
                    //The document may have been deleted, which is OK
                    if(response.isFailed() && response.getFailure().getStatus() != RestStatus.NOT_FOUND) {
                        log.error("Failed to execute ES query {}", response.getFailureMessage());
                        actualFailure = true;
                    }
                }
                if(actualFailure) {
                    throw new IOException("Failure(s) in Elasicsearch bulk request: " + bulkItemResponses.buildFailureMessage());
                }
            }
        }
    }

    @Override
    public ElasticSearchResponse search(String indexName, String type, ElasticSearchRequest request) throws IOException {
        SearchRequestBuilder srb = client.prepareSearch(indexName);
        srb.setTypes(type);
        srb.setQuery(request.getQuery());
        srb.setPostFilter(request.getPostFilter());
        if (request.getFrom() != null) {
            srb.setFrom(request.getFrom());
        }
        if (request.getSize() != null) {
            srb.setSize(request.getSize());
        }
        request.getSorts().stream().flatMap(item -> item.entrySet().stream()).forEach(item -> {
            String key = item.getKey();
            ElasticSearchRequest.RestSortInfo sortInfo = item.getValue();
            FieldSortBuilder fsb = new FieldSortBuilder(key)
                .order(SortOrder.valueOf(sortInfo.getOrder().toUpperCase()))
                .unmappedType(sortInfo.getUnmappedType());
            srb.addSort(fsb);
        });

        SearchResponse response = srb.execute().actionGet();
        SearchHits hits = response.getHits();

        List<RawQuery.Result<String>> results = new ArrayList<>(hits.hits().length);
        for (SearchHit hit : hits) {
            results.add(new RawQuery.Result<String>(hit.id(),hit.getScore()));
        }

        ElasticSearchResponse result = new ElasticSearchResponse();
        result.setTook(response.getTookInMillis());
        result.setTotal(hits.getTotalHits());
        result.setResults(results);
        return result;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public ElasticMajorVersion getMajorVersion() {
        return ElasticMajorVersion.TWO;
    }

    public void setBulkRefresh(boolean bulkRefresh) {
        this.bulkRefresh = bulkRefresh;
    }

}
