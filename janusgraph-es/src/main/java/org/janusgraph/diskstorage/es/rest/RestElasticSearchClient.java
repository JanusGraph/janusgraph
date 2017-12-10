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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.core.JsonParseException;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectReader;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectWriter;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.es.ElasticMajorVersion;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.janusgraph.diskstorage.es.IndexMappings;
import org.janusgraph.diskstorage.es.IndexMappings.IndexMapping;
import org.janusgraph.diskstorage.es.rest.RestBulkResponse.RestBulkItemResponse;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.janusgraph.util.encoding.StringEncoding.UTF8_CHARSET;

public class RestElasticSearchClient implements ElasticSearchClient {


    private static final Logger log = LoggerFactory.getLogger(RestElasticSearchClient.class);

    private static final String REQUEST_TYPE_DELETE = "DELETE";
    private static final String REQUEST_TYPE_GET = "GET";
    private static final String REQUEST_TYPE_POST = "POST";
    private static final String REQUEST_TYPE_PUT = "PUT";
    private static final String REQUEST_SEPARATOR = "/";
    private static final String REQUEST_PARAM_BEGINNING = "?";
    private static final String REQUEST_PARAM_SEPARATOR = "&";

    private static final ObjectMapper mapper;
    private static final ObjectReader mapReader;
    private static final ObjectWriter mapWriter;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(new Geoshape.GeoshapeGsonSerializerV1d0());
        mapper = new ObjectMapper();
        mapper.registerModule(module);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapReader = mapper.readerWithView(Map.class).forType(HashMap.class);
        mapWriter = mapper.writerWithView(Map.class);
    }

    private static final ElasticMajorVersion DEFAULT_VERSION = ElasticMajorVersion.FIVE;

    private static final Function<StringBuilder, StringBuilder> APPEND_OP = sb -> sb.append(sb.length() == 0 ? REQUEST_PARAM_BEGINNING : REQUEST_PARAM_SEPARATOR);

    private final RestClient delegate;

    private ElasticMajorVersion majorVersion;

    private String bulkRefresh;

    private final String scrollKeepAlive;

    public RestElasticSearchClient(RestClient delegate, int scrollKeepAlive) {
        this.delegate = delegate;
        majorVersion = getMajorVersion();
        this.scrollKeepAlive = scrollKeepAlive+"s";
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

        majorVersion = DEFAULT_VERSION;
        try {
            final Response response = delegate.performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final ClusterInfo info = mapper.readValue(inputStream, ClusterInfo.class);
                majorVersion = ElasticMajorVersion.parse(info.getVersion() != null ? (String) info.getVersion().get("number") : null);
            }
        } catch (final IOException e) {
            log.warn("Unable to determine Elasticsearch server version. Default to {}.", majorVersion, e);
        }

        return majorVersion;
    }

    @Override
    public void clusterHealthRequest(String timeout) throws IOException {
        final Map<String,String> params = ImmutableMap.of("wait_for_status","yellow","timeout", timeout);
        final Response response = delegate.performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + "_cluster" + REQUEST_SEPARATOR + "health", params);
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
            delegate.performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName);
            exists = true;
        } catch (final IOException e) {
            if (!e.getMessage().contains("404 Not Found")) {
                throw e;
            }
        }
        return exists;
    }

    @Override
    public boolean isIndex(String indexName) {
        boolean exists = false;
        try {
            final Response response = delegate.performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                exists = mapper.readValue(inputStream, Map.class).containsKey(indexName);
            }
        } catch (final IOException e) {
        }
        return exists;
    }

    @Override
    public boolean isAlias(String aliasName)  {
        boolean exists = false;
        try {
            delegate.performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + "_alias" + REQUEST_SEPARATOR + aliasName);
            exists = true;
        } catch (final IOException e) {
        }
        return exists;
    }

    @Override
    public void createIndex(String indexName, Map<String,Object> settings) throws IOException {
        performRequest(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + indexName, mapWriter.writeValueAsBytes(settings));
    }

    @Override
    public void addAlias(String alias, String index) throws IOException {
        final Map actionAlias = ImmutableMap.of("actions", ImmutableList.of(ImmutableMap.of("add", ImmutableMap.of("index", index, "alias", alias))));
        performRequest(REQUEST_TYPE_POST, REQUEST_SEPARATOR + "_aliases", mapWriter.writeValueAsBytes(actionAlias));
    }

    @Override
    public Map getIndexSettings(String indexName) throws IOException {
        final Response response = performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_settings", null);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            final Map<String,RestIndexSettings> settings = mapper.readValue(inputStream, new TypeReference<Map<String, RestIndexSettings>>() {});
            return settings == null ? null : settings.get(indexName).getSettings().getMap();
        }
    }

    @Override
    public void createMapping(String indexName, String typeName, Map<String,Object> mapping) throws IOException {
        performRequest(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping" + REQUEST_SEPARATOR + typeName, mapWriter.writeValueAsBytes(mapping));
    }

    @Override
    public IndexMapping getMapping(String indexName, String typeName) throws IOException {
        try (final InputStream inputStream = performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping" + REQUEST_SEPARATOR + typeName, null).getEntity().getContent()) {
            final Map<String, IndexMappings> settings = mapper.readValue(inputStream, new TypeReference<Map<String, IndexMappings>>() {});
            return settings != null ? settings.get(indexName).getMappings().get(typeName) : null;
        } catch (final JsonParseException | JsonMappingException | ResponseException e) {
            log.info("Error when we try to get ES mapping", e);
            return null;
        }
    }

    @Override
    public void deleteIndex(String indexName) throws IOException {
        if (majorVersion.getValue() < 6 && indexExists(indexName)) {
            performRequest(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + indexName, null);
        } else {
            final Response response = performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + "_cat" + REQUEST_SEPARATOR + "aliases" + REQUEST_PARAM_BEGINNING + "format=json", null);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final List<Map<String,Object>> records = mapper.readValue(inputStream, new TypeReference<List<Map<String, Object>>>() {});
                if (records == null) return;
                for (final Map<String,Object> record : records) {
                    final String index = (String) record.get("index");
                    if (indexExists(index)) {
                        performRequest(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + index, null);
                    }
                }
            }
        }
    }

    @Override
    public void bulkRequest(List<ElasticSearchMutation> requests, String ingestPipeline) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (final ElasticSearchMutation request : requests) {
            final Map actionData = ImmutableMap.of(request.getRequestType().name().toLowerCase(),
                    ImmutableMap.of("_index", request.getIndex(), "_type", request.getType(), "_id", request.getId()));
                outputStream.write(mapWriter.writeValueAsBytes(actionData));
            outputStream.write("\n".getBytes(UTF8_CHARSET));
            if (request.getSource() != null) {
                outputStream.write(mapWriter.writeValueAsBytes(request.getSource()));
                outputStream.write("\n".getBytes(UTF8_CHARSET));
            }
        }

        final StringBuilder builder = new StringBuilder();
        if (ingestPipeline != null) {
            APPEND_OP.apply(builder).append("pipeline=").append(ingestPipeline);
        }
        if (bulkRefresh != null && !bulkRefresh.toLowerCase().equals("false")) {
            APPEND_OP.apply(builder).append("refresh=").append(bulkRefresh);
        }
        builder.insert(0, REQUEST_SEPARATOR + "_bulk");

        final Response response = performRequest(REQUEST_TYPE_POST, builder.toString(), outputStream.toByteArray());
        try (final InputStream inputStream = response.getEntity().getContent()) {
            final RestBulkResponse bulkResponse = mapper.readValue(inputStream, RestBulkResponse.class);
            final List<Object> errors = bulkResponse.getItems().stream()
                .flatMap(item -> item.values().stream())
                .filter(item -> item.getError() != null && item.getStatus() != 404)
                .map(RestBulkItemResponse::getError).collect(Collectors.toList());
            if (!errors.isEmpty()) {
                errors.forEach(error -> log.error("Failed to execute ES query: {}", error));
                throw new IOException("Failure(s) in Elasicsearch bulk request: " + errors);
            }
        }
    }

    @Override
    public RestSearchResponse search(String indexName, String type, Map<String,Object> request, boolean useScroll) throws IOException {
        final StringBuilder path = new StringBuilder(REQUEST_SEPARATOR).append(indexName);
        if (!Strings.isNullOrEmpty(type)) {
            path.append(REQUEST_SEPARATOR).append(type);
        }
        path.append(REQUEST_SEPARATOR).append("_search");
        if (useScroll) path.append(REQUEST_PARAM_BEGINNING).append("scroll=").append(scrollKeepAlive);
        final byte[] requestData = mapper.writeValueAsBytes(request);
        if (log.isDebugEnabled()) {
            log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(request));
        }

        final Response response = performRequest(REQUEST_TYPE_POST, path.toString(), requestData);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestSearchResponse.class);
        }
    }

    @Override
    public RestSearchResponse search(String scrollId) throws IOException {
        final String path;
        final byte[] requestData;
        if (ElasticMajorVersion.ONE == majorVersion) {
             path = new StringBuilder(REQUEST_SEPARATOR).append("_search").append(REQUEST_SEPARATOR).append("scroll").append(REQUEST_PARAM_BEGINNING).append("scroll=").append(scrollKeepAlive).toString();
             requestData = scrollId.getBytes(UTF8_CHARSET);
        } else {
            path = new StringBuilder(REQUEST_SEPARATOR).append("_search").append(REQUEST_SEPARATOR).append("scroll").toString();
            final Map<String, Object> request = new HashMap<>();
            request.put("scroll", scrollKeepAlive);
            request.put("scroll_id", scrollId);
            requestData = mapper.writeValueAsBytes(request);
            if (log.isDebugEnabled()) {
                log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(request));
            }
        }
        final Response response = performRequest(REQUEST_TYPE_POST, path, requestData);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestSearchResponse.class);
        }
    }

    @Override
    public void deleteScroll(String scrollId) throws IOException {
        if (ElasticMajorVersion.ONE == majorVersion) {
            performRequest(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll", scrollId.getBytes(UTF8_CHARSET));
        } else {
            delegate.performRequest(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll" + REQUEST_SEPARATOR + scrollId);
        }
    }

    public void setBulkRefresh(String bulkRefresh) {
        this.bulkRefresh = bulkRefresh;
    }

    private Response performRequest(String method, String path, byte[] requestData) throws IOException {
        final HttpEntity entity = requestData != null ? new ByteArrayEntity(requestData, ContentType.APPLICATION_JSON) : null;
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
