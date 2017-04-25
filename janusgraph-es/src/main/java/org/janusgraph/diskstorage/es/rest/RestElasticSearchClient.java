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

package org.janusgraph.diskstorage.es.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectReader;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectWriter;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.es.ElasticMajorVersion;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.janusgraph.diskstorage.es.ElasticSearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RestElasticSearchClient implements ElasticSearchClient {

    private static final Logger log = LoggerFactory.getLogger(RestElasticSearchClient.class);

    private static final ObjectMapper mapper;
    private static final ObjectReader mapReader;
    private static final ObjectWriter mapWriter;
    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(new Geoshape.GeoshapeGsonSerializer());
        mapper = new ObjectMapper();
        mapper.registerModule(module);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapReader = mapper.readerWithView(Map.class).forType(HashMap.class);
        mapWriter = mapper.writerWithView(Map.class);
    }

    private RestClient delegate;

    private ElasticMajorVersion majorVersion;

    private String bulkRefresh;

    public RestElasticSearchClient(RestClient delegate) {
        this.delegate = delegate;
        majorVersion = getMajorVersion();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public ElasticMajorVersion getMajorVersion() {
        if (majorVersion != null) {
            return majorVersion;
        }

        final Pattern pattern = Pattern.compile("(\\d+)\\.\\d+\\.\\d+");
        majorVersion = ElasticMajorVersion.TWO;
        try {
            final Response response = delegate.performRequest("GET", "/");
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final ClusterInfo info = mapper.readValue(inputStream, ClusterInfo.class);
                final Matcher m = info.getVersion() != null ? pattern.matcher((String) info.getVersion().get("number")) : null;
                if (m == null || !m.find() || Integer.valueOf(m.group(1)) < 5) {
                    majorVersion = ElasticMajorVersion.TWO;
                } else {
                    majorVersion = ElasticMajorVersion.FIVE;
                }
            }
        } catch (Exception e) {
            log.warn("Unable to determine Elasticsearch server version. Assuming 2.x.", e);
        }

        return majorVersion;
    }

    @Override
    public void clusterHealthRequest(String timeout) throws IOException {
        Map<String,String> params = ImmutableMap.of("wait_for_status","yellow","timeout",timeout);
        final Response response = delegate.performRequest("GET", "/_cluster/health", params);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            final Map<String,Object> values = mapReader.readValue(inputStream);
            if (!values.containsKey("timed_out")) {
                throw new IOException("Unexpected response for Elasticsearch cluster health request");
            } else if (!Objects.equals(values.get("timed_out"), false)) {
                throw new IOException("Elasticsearch timeout waiting for yellow status");
            }
        }
    }

    @Override
    public boolean indexExists(String indexName) throws IOException {
        boolean exists = false;
        try {
            delegate.performRequest("GET", "/" + indexName);
            exists = true;
        } catch (IOException e) {
            if (!e.getMessage().contains("404 Not Found")) {
                throw e;
            }
        }
        return exists;
    }

    @Override
    public void createIndex(String indexName, Settings settings) throws IOException {
        performRequest("PUT", "/" + indexName, mapWriter.writeValueAsBytes(settings.getAsMap()));
    }

    @Override
    public Map getIndexSettings(String indexName) throws IOException {
        Response response = performRequest("GET", "/" + indexName + "/_settings", null);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            Map<String,RestIndexSettings> settings = mapper.readValue(inputStream, new TypeReference<Map<String, RestIndexSettings>>() {});
            return settings.get(indexName).getSettings().getMap();
        }
    }

    @Override
    public void createMapping(String indexName, String typeName, XContentBuilder mapping) throws IOException {
        byte[] bytes = mapping.bytes().toBytes();
        performRequest("PUT", "/" + indexName + "/_mapping/" + typeName, bytes);
    }

    @Override
    public Map getMapping(String indexName, String typeName) throws IOException{
        Response response = performRequest("GET", "/" + indexName.toLowerCase() + "/_mapping/"+typeName, null);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            Map<String, RestIndexMappings> settings = mapper.readValue(inputStream, new TypeReference<Map<String, RestIndexMappings>>() {});
            return settings.get(indexName).getMappings().get(typeName).getProperties();
        }
    }

    @Override
    public void deleteIndex(String indexName) throws IOException {
        try {
            performRequest("DELETE", "/" + indexName, null);
        } catch (IOException e) {
            if (!e.getMessage().contains("no such index")) {
                throw e;
            }
        }
    }

    @Override
    public void bulkRequest(List<ElasticSearchMutation> requests) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (final ElasticSearchMutation request : requests) {
            Map actionData = ImmutableMap.of(request.getRequestType().name().toLowerCase(),
                ImmutableMap.of("_index", request.getIndex(), "_type", request.getType(), "_id", request.getId()));
            outputStream.write(mapWriter.writeValueAsBytes(actionData));
            outputStream.write("\n".getBytes());
            if (request.getSource() != null) {
                outputStream.write(mapWriter.writeValueAsBytes(request.getSource()));
                outputStream.write("\n".getBytes());
            }
        }

        final StringBuilder builder = new StringBuilder("/_bulk");
        if (bulkRefresh != null && !bulkRefresh.toLowerCase().equals("false")) {
            builder.append("?refresh=" + bulkRefresh);
        }

        final Response response = performRequest("POST", builder.toString(), outputStream.toByteArray());
        try (final InputStream inputStream = response.getEntity().getContent()) {
            final RestBulkResponse bulkResponse = mapper.readValue(inputStream, RestBulkResponse.class);
            List<Map<String, Object>> errors = bulkResponse.getItems().stream()
                .flatMap(item -> item.values().stream())
                .filter(item -> item.getError() != null && item.getStatus() != RestStatus.NOT_FOUND.getStatus())
                .map(item -> item.getError()).collect(Collectors.toList());
            if (!errors.isEmpty()) {
                errors.forEach(error -> log.error("Failed to execute ES query {}", error.get("reason")));
                throw new IOException("Failure(s) in Elasicsearch bulk request: " + mapper.writeValueAsString(errors));
            }
        }
    }

    @Override
    public RestSearchResponse search(String indexName, String type, ElasticSearchRequest request) throws IOException {
        final String path = "/" + indexName + "/" + type + "/_search";

        final Map<String,Object> requestBody = new HashMap<>();

        if (request.getSize() != null) {
            requestBody.put("size",  request.getSize());
        }

        if (request.getFrom() != null) {
            requestBody.put("from", request.getFrom());
        }

        if (!request.getSorts().isEmpty()) {
            requestBody.put("sort", request.getSorts());
        }

        if (request.getQuery() != null) {
            final Map<String,Object> query = mapReader.readValue(request.getQuery().buildAsBytes().array());
            requestBody.put("query", query);
        }

        if (request.getPostFilter() != null) {
            final Map<String,Object> query = mapReader.readValue(request.getPostFilter().buildAsBytes().array());
            requestBody.put("post_filter", query);
        }

        final byte[] requestData = mapper.writeValueAsBytes(requestBody);
        if (log.isDebugEnabled()) {
            log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestBody));
        }
        Response response = performRequest("POST", path, requestData);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestSearchResponse.class);
        }
    }

    public void setBulkRefresh(String bulkRefresh) {
        this.bulkRefresh = bulkRefresh;
    }

    private Response performRequest(String method, String path, byte[] requestData) throws IOException {
        final HttpEntity entity = requestData != null ? new ByteArrayEntity(requestData) : null;
        final Response response = delegate.performRequest(
            method,
            path,
            Collections.<String, String>emptyMap(),
            entity);

        if (response.getStatusLine().getStatusCode() >= 400) {
            throw new IOException("Error executing request: " + response.getStatusLine().getReasonPhrase());
        }
        return response;
    }

    @JsonIgnoreProperties(ignoreUnknown=true)
    private static final class ClusterInfo {

        private Map<String,Object> version;

        public Map<String, Object> getVersion() {
            return version;
        }

        public void setVersion(Map<String, Object> version) {
            this.version = version;
        }

    }

}
