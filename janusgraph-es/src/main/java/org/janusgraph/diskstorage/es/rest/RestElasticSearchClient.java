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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.es.ElasticMajorVersion;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.janusgraph.diskstorage.es.mapping.IndexMapping;
import org.janusgraph.diskstorage.es.mapping.TypedIndexMappings;
import org.janusgraph.diskstorage.es.mapping.TypelessIndexMappings;
import org.janusgraph.diskstorage.es.rest.RestBulkResponse.RestBulkItemResponse;
import org.janusgraph.diskstorage.es.script.ESScriptResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
    private static final String REQUEST_TYPE_HEAD = "HEAD";
    private static final String REQUEST_SEPARATOR = "/";
    private static final String REQUEST_PARAM_BEGINNING = "?";
    private static final String REQUEST_PARAM_SEPARATOR = "&";

    public static final String INCLUDE_TYPE_NAME_PARAMETER = "include_type_name";

    private static final byte[] NEW_LINE_BYTES = "\n".getBytes(UTF8_CHARSET);

    private static final Request INFO_REQUEST = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR);

    private static final ObjectMapper mapper;
    private static final ObjectReader mapReader;
    private static final ObjectWriter mapWriter;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(new Geoshape.GeoshapeGsonSerializerV2d0());
        mapper = new ObjectMapper();
        mapper.registerModule(module);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapReader = mapper.readerWithView(Map.class).forType(HashMap.class);
        mapWriter = mapper.writerWithView(Map.class);
    }

    private static final ElasticMajorVersion DEFAULT_VERSION = ElasticMajorVersion.SEVEN;

    private static final Function<StringBuilder, StringBuilder> APPEND_OP = sb -> sb.append(sb.length() == 0 ? REQUEST_PARAM_BEGINNING : REQUEST_PARAM_SEPARATOR);

    private final RestClient delegate;

    private ElasticMajorVersion majorVersion;

    private String bulkRefresh;

    private boolean bulkRefreshEnabled = false;

    private final String scrollKeepAlive;

    private final boolean useMappingTypes;

    private final boolean esVersion7;

    private Integer retryOnConflict;

    private final String retryOnConflictKey;
    
    public RestElasticSearchClient(RestClient delegate, int scrollKeepAlive, boolean useMappingTypesForES7) {
        this.delegate = delegate;
        majorVersion = getMajorVersion();
        this.scrollKeepAlive = scrollKeepAlive+"s";
        esVersion7 = ElasticMajorVersion.SEVEN.equals(majorVersion);
        useMappingTypes = majorVersion.getValue() < 7 || (useMappingTypesForES7 && esVersion7);
        retryOnConflictKey = majorVersion.getValue() >= 7 ? "retry_on_conflict" : "_retry_on_conflict";
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
            final Response response = delegate.performRequest(INFO_REQUEST);
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
        Request clusterHealthRequest = new Request(REQUEST_TYPE_GET,
            REQUEST_SEPARATOR + "_cluster" + REQUEST_SEPARATOR + "health");
        clusterHealthRequest.addParameter("wait_for_status", "yellow");
        clusterHealthRequest.addParameter("timeout", timeout);

        final Response response = delegate.performRequest(clusterHealthRequest);
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
        final Response response = delegate.performRequest(new Request(REQUEST_TYPE_HEAD, REQUEST_SEPARATOR + indexName));
        return response.getStatusLine().getStatusCode() == 200;
    }

    @Override
    public boolean isIndex(String indexName) {
        try {
            final Response response = delegate.performRequest(new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName));
            try (final InputStream inputStream = response.getEntity().getContent()) {
                return mapper.readValue(inputStream, Map.class).containsKey(indexName);
            }
        } catch (final IOException ignored) {
        }
        return false;
    }

    @Override
    public boolean isAlias(String aliasName)  {
        try {
            delegate.performRequest(new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + "_alias" + REQUEST_SEPARATOR + aliasName));
            return true;
        } catch (final IOException ignored) {
        }
        return false;
    }

    @Override
    public void createStoredScript(String scriptName, Map<String, Object> script) throws IOException {

        Request request = new Request(REQUEST_TYPE_POST, REQUEST_SEPARATOR + "_scripts" + REQUEST_SEPARATOR + scriptName);

        performRequest(request, mapWriter.writeValueAsBytes(script));
    }

    @Override
    public ESScriptResponse getStoredScript(String scriptName) throws IOException {

        Request request = new Request(REQUEST_TYPE_GET,
            REQUEST_SEPARATOR + "_scripts" + REQUEST_SEPARATOR + scriptName);

        try{

            final Response response = delegate.performRequest(request);

            if(response.getStatusLine().getStatusCode() != 200){
                throw new IOException("Error executing request: " + response.getStatusLine().getReasonPhrase());
            }

            try (final InputStream inputStream = response.getEntity().getContent()) {

                return mapper.readValue(inputStream, new TypeReference<ESScriptResponse>() {});

            } catch (final JsonParseException | JsonMappingException | ResponseException e) {
                throw new IOException("Error when we try to parse ES script: "+response.getEntity().getContent());
            }

        } catch (ResponseException e){

            final Response response = e.getResponse();

            if(e.getResponse().getStatusLine().getStatusCode() == 404){
                ESScriptResponse esScriptResponse = new ESScriptResponse();
                esScriptResponse.setFound(false);
                return esScriptResponse;
            }

            throw new IOException("Error executing request: " + response.getStatusLine().getReasonPhrase());
        }
    }

    @Override
    public void createIndex(String indexName, Map<String,Object> settings) throws IOException {

        Request request = new Request(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + indexName);

        if(majorVersion.getValue() > 6){

            if(useMappingTypes) {
                request.addParameter(INCLUDE_TYPE_NAME_PARAMETER, "true");
            }

            if(settings != null && settings.size() > 0){
                Map<String,Object> updatedSettings = new HashMap<>();
                updatedSettings.put("settings", settings);
                settings = updatedSettings;
            }
        }

        performRequest(request, mapWriter.writeValueAsBytes(settings));
    }

    @Override
    public void updateIndexSettings(String indexName, Map<String,Object> settings) throws IOException {

        performRequest(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR +"_settings",
            mapWriter.writeValueAsBytes(settings));
    }

    @Override
    public void updateClusterSettings(Map<String, Object> settings) throws IOException {

        performRequest(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + "_cluster" + REQUEST_SEPARATOR + "settings",
            mapWriter.writeValueAsBytes(settings));
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

        Request request;

        if(useMappingTypes){
            request = new Request(REQUEST_TYPE_PUT,
                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping" + REQUEST_SEPARATOR + typeName);
            if(esVersion7){
                request.addParameter(INCLUDE_TYPE_NAME_PARAMETER, "true");
            }
        } else {
            request = new Request(REQUEST_TYPE_PUT,
                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping");
        }

        performRequest(request, mapWriter.writeValueAsBytes(mapping));
    }

    @Override
    public IndexMapping getMapping(String indexName, String typeName) throws IOException {

        Request request;

        if(useMappingTypes){
            request = new Request(REQUEST_TYPE_GET,
                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping" + REQUEST_SEPARATOR + typeName);
            if(esVersion7){
                request.addParameter(INCLUDE_TYPE_NAME_PARAMETER, "true");
            }
        } else {
            request = new Request(REQUEST_TYPE_GET,
                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping");
        }

        try (final InputStream inputStream = performRequest(request, null).getEntity().getContent()) {

            if(useMappingTypes){
                final Map<String, TypedIndexMappings> settings = mapper.readValue(inputStream,
                    new TypeReference<Map<String, TypedIndexMappings>>() {});
                return settings != null ? settings.get(indexName).getMappings().get(typeName) : null;
            }

            final Map<String, TypelessIndexMappings> settings = mapper.readValue(inputStream,
                new TypeReference<Map<String, TypelessIndexMappings>>() {});
            return settings != null ? settings.get(indexName).getMappings() : null;

        } catch (final JsonParseException | JsonMappingException | ResponseException e) {
            log.info("Error when we try to get ES mapping", e);
            return null;
        }
    }

    @Override
    public void deleteIndex(String indexName) throws IOException {
        if (isAlias(indexName)) {
            // aliased multi-index case
            final String path = REQUEST_SEPARATOR + "_alias" + REQUEST_SEPARATOR + indexName;
            final Response response = performRequest(REQUEST_TYPE_GET, path, null);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final Map<String,Object> records = mapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
                if (records == null) return;
                for (final String index : records.keySet()) {
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
            Map<String, Object> requestData = new HashMap<>();
            if (useMappingTypes) {
                requestData.put("_index", request.getIndex());
                requestData.put("_type", request.getType());
                requestData.put("_id", request.getId());
            } else {
                requestData.put("_index", request.getIndex());
                requestData.put("_id", request.getId());
            }

            if (retryOnConflict != null && request.getRequestType() == ElasticSearchMutation.RequestType.UPDATE) {
                requestData.put(retryOnConflictKey, retryOnConflict);
            }

            outputStream.write(mapWriter.writeValueAsBytes(
                ImmutableMap.of(request.getRequestType().name().toLowerCase(), requestData))
            );
            outputStream.write(NEW_LINE_BYTES);
            if (request.getSource() != null) {
                outputStream.write(mapWriter.writeValueAsBytes(request.getSource()));
                outputStream.write(NEW_LINE_BYTES);
            }
        }

        final StringBuilder builder = new StringBuilder();
        if (ingestPipeline != null) {
            APPEND_OP.apply(builder).append("pipeline=").append(ingestPipeline);
        }
        if (bulkRefreshEnabled) {
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
                throw new IOException("Failure(s) in Elasticsearch bulk request: " + errors);
            }
        }
    }

    public void setRetryOnConflict(Integer retryOnConflict) {
            this.retryOnConflict = retryOnConflict;
    }
    @Override
    public long countTotal(String indexName, Map<String, Object> requestData) throws IOException {

        final Request request = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_count");

        final byte[] requestDataBytes = mapper.writeValueAsBytes(requestData);
        if (log.isDebugEnabled()) {
            log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestData));
        }

        final Response response = performRequest(request, requestDataBytes);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestCountResponse.class).getCount();
        }
    }

    @Override
    public RestSearchResponse search(String indexName, Map<String,Object> requestData, boolean useScroll) throws IOException {
        final StringBuilder path = new StringBuilder(REQUEST_SEPARATOR).append(indexName);
        path.append(REQUEST_SEPARATOR).append("_search");
        if (useScroll) {
            path.append(REQUEST_PARAM_BEGINNING).append("scroll=").append(scrollKeepAlive);
        }
        return search(requestData, path.toString());
    }

    @Override
    public RestSearchResponse search(String scrollId) throws IOException {
        final Map<String, Object> requestData = new HashMap<>();
        requestData.put("scroll", scrollKeepAlive);
        requestData.put("scroll_id", scrollId);
        return search(requestData, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll");
    }

    @Override
    public void deleteScroll(String scrollId) throws IOException {
        delegate.performRequest(new Request(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll" + REQUEST_SEPARATOR + scrollId));
    }

    public void setBulkRefresh(String bulkRefresh) {
        this.bulkRefresh = bulkRefresh;
        bulkRefreshEnabled = bulkRefresh != null && !bulkRefresh.equalsIgnoreCase("false");
    }

    private RestSearchResponse search(Map<String, Object> requestData, String path) throws IOException {

        final Request request = new Request(REQUEST_TYPE_POST, path);

        final byte[] requestDataBytes = mapper.writeValueAsBytes(requestData);
        if (log.isDebugEnabled()) {
            log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestData));
        }

        final Response response = performRequest(request, requestDataBytes);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestSearchResponse.class);
        }
    }

    private Response performRequest(String method, String path, byte[] requestData) throws IOException {
        return performRequest(new Request(method, path), requestData);
    }

    private Response performRequest(Request request, byte[] requestData) throws IOException {

        final HttpEntity entity = requestData != null ? new ByteArrayEntity(requestData, ContentType.APPLICATION_JSON) : null;

        request.setEntity(entity);

        final Response response = delegate.performRequest(request);

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
