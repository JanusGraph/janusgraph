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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
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
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.es.ElasticMajorVersion;
import org.janusgraph.diskstorage.es.ElasticSearchBulkFailureException;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.janusgraph.diskstorage.es.TransientFailures;
import org.janusgraph.diskstorage.es.mapping.IndexMapping;
import org.janusgraph.diskstorage.es.mapping.TypedIndexMappings;
import org.janusgraph.diskstorage.es.mapping.TypelessIndexMappings;
import org.janusgraph.diskstorage.es.script.ESScriptResponse;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.janusgraph.util.encoding.StringEncoding.UTF8_CHARSET;

public class RestElasticSearchClient implements ElasticSearchClient {

    private static final Logger log = LoggerFactory.getLogger(RestElasticSearchClient.class);

    private static final String REQUEST_TYPE_DELETE = "DELETE";
    private static final String REQUEST_TYPE_GET = "GET";
    private static final String REQUEST_TYPE_POST = "POST";
    //The type Elasticsearch names in the error of a bulk item which was answered with a 404 because the index is gone
    private static final String INDEX_NOT_FOUND_EXCEPTION = "index_not_found_exception";
    private static final String ERROR_TYPE_KEY = "type";
    private static final String REQUEST_TYPE_PUT = "PUT";
    private static final String REQUEST_TYPE_HEAD = "HEAD";
    private static final String REQUEST_SEPARATOR = "/";
    private static final String REQUEST_PARAM_BEGINNING = "?";
    private static final String REQUEST_PARAM_SEPARATOR = "&";

    public static final String INCLUDE_TYPE_NAME_PARAMETER = "include_type_name";

    private static final byte[] NEW_LINE_BYTES = "\n".getBytes(UTF8_CHARSET);

    //Room for the action line of an item whose index name and id are of usual lengths, without growing
    private static final int ACTION_LINE_SIZE = 128;

    //The fields of a bulk response which the client asks for: each item's status and error, and errors, which tells
    //whether any item failed. Every item keeps its status, so the items still line up with the requests they answer;
    //everything else Elasticsearch writes for each item (index, id, version, result, shards, sequence number, primary
    //term) would only be transferred and parsed to be thrown away. The actions are named rather than matched with a
    //wildcard, so that the query string holds no asterisk, which a request signer would have to percent-encode the way
    //the cluster does for the signature to match
    private static final String BULK_RESPONSE_FILTER = "errors,"
        + Arrays.stream(ElasticSearchMutation.RequestType.values())
            .map(RestElasticSearchClient::action)
            .flatMap(action -> Stream.of("items." + action + ".status", "items." + action + ".error"))
            .collect(Collectors.joining(","));

    private static final Request INFO_REQUEST = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR);

    //The major and minor number of a version the cluster reports, such as 7.17.8 or 8.0.0-SNAPSHOT
    private static final Pattern VERSION_NUMBER = Pattern.compile("(\\d+)\\.(\\d+).*");

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

    private static final ElasticMajorVersion DEFAULT_VERSION = ElasticMajorVersion.NINE;

    private static final Function<StringBuilder, StringBuilder> APPEND_OP = sb -> sb.append(sb.length() == 0 ? REQUEST_PARAM_BEGINNING : REQUEST_PARAM_SEPARATOR);

    private final RestClient delegate;

    private ElasticMajorVersion majorVersion;

    private String bulkRefresh;

    private boolean bulkRefreshEnabled = false;

    private final String scrollKeepAlive;

    private final boolean useMappingTypes;

    //OpenSearch 2 removed the mapping types which Elasticsearch 7 still supports, and JanusGraph supports OpenSearch 2
    //and newer only
    private boolean mappingTypesRemoved;

    private final boolean esVersion7;

    private Integer retryOnConflict;

    //Whether a failure which produced no HTTP response is reattempted like a status code in retryOnErrorCodes.
    //Configured through RestClientSetup from RETRY_TRANSPORT_FAILURES, like the other optional client settings
    private boolean retryTransportFailures;

    private final int retryAttemptLimit;

    private final Set<Integer> retryOnErrorCodes;

    private final long retryInitialWaitMs;

    private final long retryMaxWaitMs;

    private final int bulkChunkSerializedLimitBytes;
    //Set once the first rejected release of a scroll or a point in time has been logged as a warning. Every one of
    //the index backend is released through this client with the same credentials, so later rejections repeat the
    //same problem and are logged at debug level. Releases complete on the HTTP client's I/O threads, hence the atomic
    //flag
    private final AtomicBoolean warnedAboutRejectedRelease = new AtomicBoolean();

    //Whether the cluster can page a result with a point in time: Elasticsearch 7.12 introduced the implicit _shard_doc
    //tiebreaker which search_after over a point in time relies on
    private boolean pointInTimeSupported;

public RestElasticSearchClient(RestClient delegate, int scrollKeepAlive, boolean useMappingTypesForES7,
                               int retryAttemptLimit, Set<Integer> retryOnErrorCodes, long retryInitialWaitMs,
                               long retryMaxWaitMs, int bulkChunkSerializedLimitBytes) {
        this(delegate, scrollKeepAlive, useMappingTypesForES7, retryAttemptLimit, retryOnErrorCodes, retryInitialWaitMs,
            retryMaxWaitMs, bulkChunkSerializedLimitBytes, null);
    }

    /**
     * @param configuredMajorVersion the major version of the Elasticsearch API to use, or {@code null} to ask the
     * cluster for it
     */
    public RestElasticSearchClient(RestClient delegate, int scrollKeepAlive, boolean useMappingTypesForES7,
                                   int retryAttemptLimit, Set<Integer> retryOnErrorCodes, long retryInitialWaitMs,
                                   long retryMaxWaitMs, int bulkChunkSerializedLimitBytes,
                                   ElasticMajorVersion configuredMajorVersion) {
        this.delegate = delegate;
        if (configuredMajorVersion != null) {
            majorVersion = configuredMajorVersion;
            //Without asking the cluster, only a major version whose every release has the point in time API is known
            //to have it
            pointInTimeSupported = supportsPointInTimeThroughout(configuredMajorVersion);
        } else {
            majorVersion = getMajorVersion();
        }
        this.scrollKeepAlive = scrollKeepAlive+"s";
        esVersion7 = ElasticMajorVersion.SEVEN.equals(majorVersion);
        if (useMappingTypesForES7 && mappingTypesRemoved) {
            log.warn("The option index.[X].elasticsearch.{} is ignored: the cluster is OpenSearch, and OpenSearch 2 and " +
                "newer have no mapping types.",
                ElasticSearchIndex.USE_MAPPING_FOR_ES7.getName());
        }
        useMappingTypes = majorVersion.getValue() < 7 || (useMappingTypesForES7 && esVersion7 && !mappingTypesRemoved);
        this.retryAttemptLimit = retryAttemptLimit;
        this.retryOnErrorCodes = Collections.unmodifiableSet(retryOnErrorCodes);
        this.retryInitialWaitMs = retryInitialWaitMs;
        this.retryMaxWaitMs = retryMaxWaitMs;
        this.bulkChunkSerializedLimitBytes = bulkChunkSerializedLimitBytes;
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
        //A cluster whose version can't be asked for isn't known to have a point in time, so it scrolls
        pointInTimeSupported = false;
        try {
            final Response response = delegate.performRequest(INFO_REQUEST);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final ClusterInfo info = mapper.readValue(inputStream, ClusterInfo.class);
                try {
                    majorVersion = ElasticMajorVersion.fromServerVersion(info.getVersion());
                } catch (final IllegalArgumentException e) {
                    throw new IllegalArgumentException(e.getMessage() + ". Set index.[X].elasticsearch." +
                        ElasticSearchIndex.MAJOR_VERSION.getName() + " if the cluster provides the API of a supported " +
                        "Elasticsearch major version.", e);
                }
                pointInTimeSupported = clusterSupportsPointInTime(info.getVersion());
                mappingTypesRemoved = ElasticMajorVersion.isOpenSearch(info.getVersion());
                if (ElasticMajorVersion.isOpenSearch(info.getVersion())) {
                    log.info("OpenSearch {} provides the Elasticsearch {} API, which JanusGraph uses.",
                        info.getVersion().get("number"), majorVersion.getValue());
                }
            }
        } catch (final IOException e) {
            log.warn("Unable to determine Elasticsearch server version. Default to {}, and to scrolls for results larger " +
                "than a page. Set index.[X].elasticsearch.{} to skip the detection.", majorVersion,
                ElasticSearchIndex.MAJOR_VERSION.getName(), e);
        }

        return majorVersion;
    }

    @Override
    public boolean usesMappingTypes() {
        return useMappingTypes;
    }

    /**
     * Whether a cluster with the given {@code version} object of its root endpoint can page a result with a point in
     * time: Elasticsearch 7.12 introduced the implicit {@code _shard_doc} tiebreaker which {@code search_after} over a
     * point in time relies on. OpenSearch has a point in time API since 2.4, but neither the tiebreaker nor sort values
     * for a search sorted by score, so {@code search_after} can't page a query which doesn't sort by a field of its own
     * there, and OpenSearch scrolls in every paging mode (verified on OpenSearch 2.19 and 3.9).
     */
    public static boolean clusterSupportsPointInTime(Map<String, Object> version) {
        if (ElasticMajorVersion.isOpenSearch(version)) {
            return false;
        }
        final Object number = version != null ? version.get("number") : null;
        final Matcher matcher = number instanceof String ? VERSION_NUMBER.matcher((String) number) : null;
        if (matcher == null || !matcher.matches()) {
            return false;
        }
        final int major = Integer.parseInt(matcher.group(1));
        final int minor = Integer.parseInt(matcher.group(2));
        return major > 7 || (major == 7 && minor >= 12);
    }

    //Whether every release of an Elasticsearch major version has the point in time API: 8 and later
    private static boolean supportsPointInTimeThroughout(ElasticMajorVersion majorVersion) {
        return majorVersion.getValue() >= 8;
    }

    @Override
    public boolean supportsPointInTime() {
        return pointInTimeSupported;
    }

    @Override
    public String openPointInTime(String indexName) throws IOException {
        final Request request = new Request(REQUEST_TYPE_POST, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_pit");
        request.addParameter("keep_alive", scrollKeepAlive);
        final Response response = performRequest(request, null);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestPointInTimeResponse.class).getId();
        }
    }

    @Override
    public RestSearchResponse searchPointInTime(String pitId, Map<String, Object> requestData) throws IOException {
        //A search of a point in time names its indexes through the point in time, not in the path
        requestData.put("pit", ImmutableMap.of("id", pitId, "keep_alive", scrollKeepAlive));
        return search(requestData, REQUEST_SEPARATOR + "_search");
    }

    @Override
    public void closePointInTime(String pitId) throws IOException {
        release(new Request(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + "_pit"), ImmutableMap.of("id", pitId),
            "point in time", pitId);
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

            if(e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND){
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
    public void clearStore(String indexStoreName) throws IOException {
        if (indexExists(indexStoreName)) {
            performRequest(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + indexStoreName, null);
        }
    }

    @VisibleForTesting
    class RequestBytes {
        final byte [] requestBytes;
        final byte [] requestSource;
        //Retained so that a failed bulk item can be interpreted against the operation which produced it
        final boolean removesContentOnly;
        //The document the item belongs to, so that a failed bulk can say which documents did not apply
        final String store;
        final String documentId;

        @VisibleForTesting
        RequestBytes(final ElasticSearchMutation request) throws IOException {
            this.removesContentOnly = request.removesContentOnly();
            this.store = request.getType();
            this.documentId = request.getId();
            this.requestBytes = actionLine(request);
            if (request.getSource() != null) {
                this.requestSource = mapWriter.writeValueAsBytes(request.getSource());
            } else {
                this.requestSource = null;
            }
        }

        //The action line, written field by field. Serializing a map of its fields through the object mapper cost a map,
        //a serializer lookup and its own generator for each item, about as much as the item's source
        private byte[] actionLine(final ElasticSearchMutation request) throws IOException {
            final ByteArrayOutputStream line = new ByteArrayOutputStream(ACTION_LINE_SIZE);
            try (JsonGenerator generator = mapper.getFactory().createGenerator(line)) {
                generator.writeStartObject();
                generator.writeObjectFieldStart(action(request.getRequestType()));
                generator.writeStringField("_index", request.getIndex());
                if (useMappingTypes) {
                    generator.writeStringField("_type", request.getType());
                }
                generator.writeStringField("_id", request.getId());
                //Elasticsearch's own default is 0, so a request needs the key only above it. Every supported version
                //reads retry_on_conflict; Elasticsearch 6 merely deprecated the _retry_on_conflict it also accepted
                if (retryOnConflict != null && retryOnConflict > 0
                    && request.getRequestType() == ElasticSearchMutation.RequestType.UPDATE) {
                    generator.writeNumberField("retry_on_conflict", retryOnConflict);
                }
                generator.writeEndObject();
                generator.writeEndObject();
            }
            return line.toByteArray();
        }

        @VisibleForTesting
        int getSerializedSize() {
            int serializedSize = this.requestBytes.length;
            serializedSize+= 1; //For follow-up NEW_LINE_BYTES
            if (this.requestSource != null) {
                serializedSize += this.requestSource.length;
                serializedSize+= 1; //For follow-up NEW_LINE_BYTES
            }
            return serializedSize;
        }

        private void writeTo(OutputStream outputStream) throws IOException {
            outputStream.write(this.requestBytes);
            outputStream.write(NEW_LINE_BYTES);
            if (this.requestSource != null) {
                outputStream.write(requestSource);
                outputStream.write(NEW_LINE_BYTES);
            }
        }
    }

    //The name of an action in a bulk request, in the root locale: in a Turkish one INDEX lower-cases to ındex
    private static String action(final ElasticSearchMutation.RequestType requestType) {
        return requestType.name().toLowerCase(Locale.ROOT);
    }

    private String bulkRequestPath(String ingestPipeline) {
        final StringBuilder bulkRequestQueryParameters = new StringBuilder();
        if (ingestPipeline != null) {
            APPEND_OP.apply(bulkRequestQueryParameters).append("pipeline=").append(ingestPipeline);
        }
        if (bulkRefreshEnabled) {
            APPEND_OP.apply(bulkRequestQueryParameters).append("refresh=").append(bulkRefresh);
        }
        return REQUEST_SEPARATOR + "_bulk" + bulkRequestQueryParameters;
    }

    //The body of a bulk request, which it writes from the items' serialized bytes as they are. Copying them into one
    //array first, through a ByteArrayOutputStream which grows by doubling and then copies itself once more, held up to
    //three more copies of the body at once, for each attempt. It can be sent again, to another node or by a
    //reattempt, and it knows its length up front
    @VisibleForTesting
    static final class BulkRequestEntity extends AbstractHttpEntity {
        private final List<RequestBytes> requests;
        private final long contentLength;

        BulkRequestEntity(final List<RequestBytes> requests) {
            this.requests = requests;
            long length = 0;
            for (final RequestBytes request : requests) {
                length += request.getSerializedSize();
            }
            this.contentLength = length;
            setContentType(ContentType.APPLICATION_JSON.toString());
        }

        @Override
        public boolean isRepeatable() {
            return true;
        }

        @Override
        public long getContentLength() {
            return contentLength;
        }

        @Override
        public InputStream getContent() {
            return new BulkRequestInputStream(requests, contentLength);
        }

        @Override
        public void writeTo(final OutputStream outputStream) throws IOException {
            for (final RequestBytes request : requests) {
                request.writeTo(outputStream);
            }
        }

        @Override
        public boolean isStreaming() {
            return false;
        }
    }

    //Reads the items' bytes in the order the entity writes them: each item's action line and a new line, then its
    //source and another new line if it has one. It behaves as a ByteArrayInputStream over the whole body would: a read
    //fills as much of the buffer as it can, across items, available() counts what is left, and it supports mark and
    //reset, which request signers such as those of the AWS SDK use to hash the body before it is sent
    private static final class BulkRequestInputStream extends InputStream {
        //The parts of an item: its action line, a new line, its source and a new line. An item without a source has
        //only the first two
        private static final int ACTION = 0;
        private static final int SOURCE = 2;
        private static final int PARTS = 4;

        private final List<RequestBytes> requests;
        //Where the stream is: the item, -1 before the first, its part, PARTS before the first and at the end, the
        //position in the part, and the bytes left
        private int request = -1;
        private int part = PARTS;
        private int position;
        private long remaining;
        private byte[] current;
        //Where reset() goes back to, the start unless mark() was called
        private int markedRequest = -1;
        private int markedPart = PARTS;
        private int markedPosition;
        private long markedRemaining;

        private BulkRequestInputStream(final List<RequestBytes> requests, final long contentLength) {
            this.requests = requests;
            this.remaining = contentLength;
            this.markedRemaining = contentLength;
        }

        private byte[] partBytes() {
            if (request < 0 || part == PARTS) {
                return null;
            }
            final RequestBytes item = requests.get(request);
            return part == ACTION ? item.requestBytes : part == SOURCE ? item.requestSource : NEW_LINE_BYTES;
        }

        //Moves on to the next part which has bytes left; false at the end of the body
        private boolean hasRemaining() {
            while (current == null || position == current.length) {
                if (request >= 0 && part < PARTS) {
                    part++;
                    if (part == SOURCE && requests.get(request).requestSource == null) {
                        part = PARTS;
                    }
                }
                if (part == PARTS) {
                    if (request + 1 >= requests.size()) {
                        return false;
                    }
                    request++;
                    part = ACTION;
                }
                current = partBytes();
                position = 0;
            }
            return true;
        }

        @Override
        public int read() {
            if (!hasRemaining()) {
                return -1;
            }
            remaining--;
            return current[position++] & 0xFF;
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) {
            Objects.checkFromIndexSize(offset, length, buffer.length);
            if (length == 0) {
                return 0;
            }
            int read = 0;
            while (read < length && hasRemaining()) {
                final int count = Math.min(length - read, current.length - position);
                System.arraycopy(current, position, buffer, offset + read, count);
                position += count;
                read += count;
            }
            remaining -= read;
            return read == 0 ? -1 : read;
        }

        @Override
        public int available() {
            return (int) Math.min(remaining, Integer.MAX_VALUE);
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        //The whole body is in memory, so a mark holds however far the stream is read after it
        @Override
        public void mark(final int readLimit) {
            markedRequest = request;
            markedPart = part;
            markedPosition = position;
            markedRemaining = remaining;
        }

        @Override
        public void reset() {
            request = markedRequest;
            part = markedPart;
            position = markedPosition;
            remaining = markedRemaining;
            current = partBytes();
        }
    }

    private List<Triplet<Object, Integer, RequestBytes>> pairErrorsWithSubmittedMutation(
        //Bulk API is documented to return bulk item responses in the same order of submission
        //https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-api-response-body
        //As such we only need to retry elements that failed
        final List<Map<String, RestBulkResponse.RestBulkItemResponse>> bulkResponseItems,
        final List<RequestBytes> submittedBulkRequestItems) {
        final List<Triplet<Object, Integer, RequestBytes>> errors = new ArrayList<>(bulkResponseItems.size());
        for (int itemIndex = 0; itemIndex < bulkResponseItems.size(); itemIndex++) {
            Collection<RestBulkResponse.RestBulkItemResponse> bulkResponseItem = bulkResponseItems.get(itemIndex).values();
            if (bulkResponseItem.size() > 1) {
                throw new IllegalStateException("There should only be a single item per bulk response item entry");
            }
            RestBulkResponse.RestBulkItemResponse item = bulkResponseItem.iterator().next();
            final RequestBytes submittedItem = submittedBulkRequestItems.get(itemIndex);
            if (item.getError() != null && !isAbsentDocumentRemoval(item, submittedItem)) {
                errors.add(Triplet.with(item.getError(), item.getStatus(), submittedItem));
            }
        }
        return errors;
    }

    //Removing content which is already absent leaves the index in the state the mutation asked for, so the 404
    //Elasticsearch answers with is a success. Both a whole document deletion and a script which deletes fields count.
    //A 404 for a mutation which adds content is a document_missing_exception: the write did not happen, and treating
    //it as a success drops the mutation with nothing reported.
    //The one 404 a removal is not exempt from is the index_not_found_exception Elasticsearch answers a deletion
    //against a missing index with: the whole index being gone is not a state any mutation asked for, and an addition
    //against the same index is reported, so a removal is too. The reasons a 404 otherwise carries here: a whole
    //document deletion of an absent document does not reach this method at all, because Elasticsearch answers it
    //with result not_found and no error; the field deletion script gets a document_missing_exception, which is
    //exempt; and an update against a missing index, with the default action.auto_create_index, creates the index and
    //then reports the document as missing
    private static boolean isAbsentDocumentRemoval(final RestBulkResponse.RestBulkItemResponse item,
                                                   final RequestBytes submittedItem) {
        return item.getStatus() == HttpStatus.SC_NOT_FOUND && submittedItem.removesContentOnly
            && !isIndexNotFound(item.getError());
    }

    private static Map<String, Set<String>> documentsByStore(final Iterable<RequestBytes> requests) {
        final Map<String, Set<String>> documentsByStore = new HashMap<>();
        for (final RequestBytes request : requests) {
            documentsByStore.computeIfAbsent(request.store, k -> new HashSet<>()).add(request.documentId);
        }
        return documentsByStore;
    }

    //The error of a failed bulk item is a map which names the exception under "type"
    private static boolean isIndexNotFound(final Object error) {
        return error instanceof Map && INDEX_NOT_FOUND_EXCEPTION.equals(((Map<?, ?>) error).get(ERROR_TYPE_KEY));
    }

    @VisibleForTesting
    class BulkRequestChunker implements Iterator<List<RequestBytes>> {
        //By default, Elasticsearch writes are limited to 100mb, so chunk a given batch of requests so they stay under
        //the specified limit

        //https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-desc
        //There is no "correct" number of actions to perform in a single bulk request. Experiment with different
        // settings to find the optimal size for your particular workload. Note that Elasticsearch limits the maximum
        // size of a HTTP request to 100mb by default
        private final PeekingIterator<RequestBytes> requestIterator;
        private final int[] exceptionallyLargeRequests;
        //The documents of the oversized requests, by store: never sent, and reported only once every well sized chunk
        //went through, so a failure before that has to name them as unsent
        private final Map<String, Set<String>> oversizedDocumentsByStore = new HashMap<>();

        @VisibleForTesting
        BulkRequestChunker(List<ElasticSearchMutation> requests) throws IOException {
            List<RequestBytes> serializedRequests = new ArrayList<>(requests.size());
            List<Integer> requestSizesThatWereTooLarge = new ArrayList<>();
            for (ElasticSearchMutation request : requests) {
                RequestBytes requestBytes = new RequestBytes(request);
                int requestSerializedSize = requestBytes.getSerializedSize();
                final ElasticSearchMutation withoutCompleteDocument = request.withoutCompleteDocument();
                if (requestSerializedSize > bulkChunkSerializedLimitBytes && withoutCompleteDocument != null) {
                    //The element's complete document only matters when the document turns out to be missing. An update
                    //it makes too large to send goes without it, and updates an existing document exactly the same way
                    requestBytes = new RequestBytes(withoutCompleteDocument);
                    requestSerializedSize = requestBytes.getSerializedSize();
                }
                if (requestSerializedSize <= bulkChunkSerializedLimitBytes) {
                    //Only keep items that we can actually send in memory
                    serializedRequests.add(requestBytes);
                } else {
                    requestSizesThatWereTooLarge.add(requestSerializedSize);
                    oversizedDocumentsByStore.computeIfAbsent(request.getType(), k -> new HashSet<>()).add(request.getId());
                }
            }
            this.requestIterator = Iterators.peekingIterator(serializedRequests.iterator());
            //Condense request sizes that are too large into an int array to remove Boxed & List memory overhead
            this.exceptionallyLargeRequests = requestSizesThatWereTooLarge.isEmpty() ? null :
                requestSizesThatWereTooLarge.stream().mapToInt(Integer::intValue).toArray();
        }

        //The documents which have not been sent, by store: those of the chunks not handed out yet, and the oversized
        //ones. A failure names them as unsent so that a reattempt does not take them for applied and drop them. This
        //consumes the chunker, so it is asked once, when a failure ends the request
        Map<String, Set<String>> unsentDocumentsByStore() {
            final Map<String, Set<String>> unsent = new HashMap<>();
            requestIterator.forEachRemaining(request ->
                unsent.computeIfAbsent(request.store, k -> new HashSet<>()).add(request.documentId));
            oversizedDocumentsByStore.forEach((store, ids) -> unsent.computeIfAbsent(store, k -> new HashSet<>()).addAll(ids));
            return unsent;
        }

        @Override
        public boolean hasNext() {
            //Make sure hasNext() still returns true if exceptionally large requests were attempted to be submitted
            //This allows next() to throw after all well sized requests have been chunked for submission
            return requestIterator.hasNext() || exceptionallyLargeRequests != null;
        }

        @Override
        public List<RequestBytes> next() {
            List<RequestBytes> serializedRequests = new ArrayList<>();
            int chunkSerializedTotal = 0;
            while (requestIterator.hasNext()) {
                RequestBytes peeked = requestIterator.peek();
                chunkSerializedTotal += peeked.getSerializedSize();
                if (chunkSerializedTotal <= bulkChunkSerializedLimitBytes) {
                    serializedRequests.add(requestIterator.next());
                } else {
                    //Adding this element would exceed the limit, so return the chunk
                    return serializedRequests;
                }
            }
            //Check if we should throw an exception for items that were exceptionally large and therefore undeliverable.
            //This is only done after all items that could be sent have been sent
            if (serializedRequests.isEmpty() && this.exceptionallyLargeRequests != null) {
                throw new IllegalArgumentException(String.format(
                    "Bulk request item(s) larger than permitted chunk limit. Limit is %s. Serialized item size(s) %s",
                    bulkChunkSerializedLimitBytes, Arrays.toString(this.exceptionallyLargeRequests)));
            }
            //All remaining requests fit in this chunk
            return serializedRequests;
        }
    }

    @Override
    public void bulkRequest(final List<ElasticSearchMutation> requests, String ingestPipeline) throws IOException {
        final String bulkRequestPath = bulkRequestPath(ingestPipeline);
        BulkRequestChunker bulkRequestChunker = new BulkRequestChunker(requests);
        while (bulkRequestChunker.hasNext()) {
            List<RequestBytes> bulkRequestChunk = bulkRequestChunker.next();
            int retryCount = 0;
            while (true) {
                final Request bulkRequest = new Request(REQUEST_TYPE_POST, bulkRequestPath);
                bulkRequest.addParameter("filter_path", BULK_RESPONSE_FILTER);
                final Response response = performRequestWithEntity(bulkRequest, new BulkRequestEntity(bulkRequestChunk));
                try (final InputStream inputStream = response.getEntity().getContent()) {
                    final RestBulkResponse bulkResponse = mapper.readValue(inputStream, RestBulkResponse.class);
                    List<Triplet<Object, Integer, RequestBytes>> bulkItemsThatFailed = pairErrorsWithSubmittedMutation(bulkResponse.getItems(), bulkRequestChunk);
                    if (!bulkItemsThatFailed.isEmpty()) {
                        //Only retry the bulk request if *all* the bulk response item error codes are retry error codes
                        final Set<Integer> errorCodes = bulkItemsThatFailed.stream().map(Triplet::getValue1).collect(Collectors.toSet());
                        if (retryCount < retryAttemptLimit && retryOnErrorCodes.containsAll(errorCodes)) {
                            //Build up the next request batch, of only the failed mutations
                            bulkRequestChunk = bulkItemsThatFailed.stream().map(Triplet::getValue2).collect(Collectors.toList());
                            performRetryWait(retryCount);
                            retryCount++;
                        } else {
                            final List<Object> errorItems = bulkItemsThatFailed.stream().map(Triplet::getValue0).collect(Collectors.toList());
                            //Summarise rather than log a line per item: a large batch rejected wholesale would
                            //otherwise emit thousands of lines, once per reattempt, exactly during the outage the
                            //reattempts exist for. The level which matches the outcome of the whole mutation is the
                            //caller's to choose, and the thrown exception carries every failed item on a getter
                            log.warn("{} of {} items in the Elasticsearch bulk request failed, with statuses {}",
                                bulkItemsThatFailed.size(), bulkRequestChunk.size(), errorCodes);
                            if (log.isDebugEnabled()) {
                                for (final Triplet<Object, Integer, RequestBytes> failedItem : bulkItemsThatFailed) {
                                    log.debug("Failed to execute ES query with status {}: {}",
                                        failedItem.getValue1(), failedItem.getValue0());
                                }
                            }
                            final List<RequestBytes> failedRequests = bulkItemsThatFailed.stream()
                                .map(Triplet::getValue2).collect(Collectors.toList());
                            //Retain the item statuses so callers can classify the failure as transient or permanent,
                            //and the documents so that a reattempt can leave out those which applied: the ones whose
                            //items failed here, and the ones of the chunks this failure stops from being sent at all
                            throw new ElasticSearchBulkFailureException(errorCodes, errorItems,
                                documentsByStore(failedRequests), bulkRequestChunker.unsentDocumentsByStore());
                        }
                    } else {
                        //The entire bulk request was successful, leave the loop
                        break;
                    }
                }
            }
        }
    }

    public void setRetryTransportFailures(boolean retryTransportFailures) {
        this.retryTransportFailures = retryTransportFailures;
    }

    public void setRetryOnConflict(Integer retryOnConflict) {
            this.retryOnConflict = retryOnConflict;
    }

    @Override
    public long countTotal(String indexName, Map<String, Object> requestData) throws IOException {
        return countTotal(indexName, requestData, 0);
    }

    @Override
    public long countTotal(String indexName, Map<String, Object> requestData, int atMost) throws IOException {

        final Request request = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_count");
        if (atMost > 0) {
            // each shard stops collecting at the bound, so a count which only has to reach a limit doesn't count everything
            request.addParameter("terminate_after", String.valueOf(atMost));
        }

        final byte[] requestDataBytes = mapper.writeValueAsBytes(requestData);
        if (log.isDebugEnabled()) {
            log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestData));
        }

        final Response response = performRequest(request, requestDataBytes);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestCountResponse.class).getCount();
        }
    }

    /**
     * Execute the aggregation request using Elasticsearch index.
     * Elasticsearch uses double values to hold and represent numeric data. As a result, aggregations on long numbers
     * greater than 2^53 are approximate.
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-aggregations.html#limits-for-long-values">Elasticsearch, limits for long values</a>
     *
     * @param indexName the name of the ElasticSearch index on which the aggregation is executed
     * @param requestData the filter query
     * @param agg the name of the aggregation operation (min, max, avg, sum)
     * @param fieldName the name of the field on which the aggregation is computed
     * @return the result of the aggregation
     * @throws IOException
     */
    private double executeAggs(String indexName, Map<String, Object> requestData, String agg, String fieldName) throws IOException {

        final Request request = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_search");

        requestData.put("aggs", ImmutableMap.of("agg_result", ImmutableMap.of(agg, ImmutableMap.of("field", fieldName))));
        // only the aggregation is wanted: no hits, which would come with their sources, and no count of the matches
        requestData.put("size", 0);
        requestData.put("track_total_hits", false);
        final byte[] requestDataBytes = mapper.writeValueAsBytes(requestData);
        if (log.isDebugEnabled()) {
            log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestData));
        }

        final Response response = performRequest(request, requestDataBytes);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestAggResponse.class).getAggregations().getAggResult().getValue();
        }
    }

    private Number adaptNumberType(double value, Class<? extends Number> expectedType) {
        if (expectedType == null) return value;
        else if (Byte.class.isAssignableFrom(expectedType)) return (byte)value;
        else if (Short.class.isAssignableFrom(expectedType)) return (short)value;
        else if (Integer.class.isAssignableFrom(expectedType)) return (int)value;
        else if (Long.class.isAssignableFrom(expectedType)) return (long)value;
        else if (Float.class.isAssignableFrom(expectedType)) return (float)value;
        else return value;
    }

    @Override
    public Number min(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException {
        return adaptNumberType(executeAggs(indexName, requestData, "min", fieldName), expectedType);
    }

    @Override
    public Number max(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException {
        return adaptNumberType(executeAggs(indexName, requestData, "max", fieldName), expectedType);
    }

    @Override
    public double avg(String indexName, Map<String, Object> requestData, String fieldName) throws IOException {
        return executeAggs(indexName, requestData, "avg", fieldName);
    }

    @Override
    public Number sum(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException {
        Class<? extends Number> returnType;
        double sum = executeAggs(indexName, requestData, "sum", fieldName);
        if (Float.class.isAssignableFrom(expectedType) || Double.class.isAssignableFrom(expectedType))
            return sum;
        else
            return (long)sum;
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
        //The id goes in the body: the path form has been deprecated since Elasticsearch 7, and an id can outgrow a URL
        release(new Request(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll"),
            ImmutableMap.of("scroll_id", ImmutableList.of(scrollId)), "scroll", scrollId);
    }

    //Releases a scroll context or a point in time. Releasing shouldn't cost the search a round trip, so the request
    //goes out without waiting for its answer. Should it be lost, the context expires after the keep-alive anyway
    private void release(Request request, Map<String, Object> body, String kind, String id) throws IOException {
        request.setEntity(new ByteArrayEntity(mapWriter.writeValueAsBytes(body), ContentType.APPLICATION_JSON));
        delegate.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
            }

            @Override
            public void onFailure(Exception exception) {
                //A release the cluster rejects, for example for want of the privilege to clear scrolls, means every
                //context this client opens stays open until it expires, which is worth one warning. A lost request,
                //a cluster which is momentarily unable to answer, or a context which had expired already isn't
                if (exception instanceof ResponseException
                    && isRejectedRelease(((ResponseException) exception).getResponse().getStatusLine().getStatusCode())
                    && !warnedAboutRejectedRelease.getAndSet(true)) {
                    log.warn("Elasticsearch rejected the release of the {} {}, so such contexts stay open until they " +
                        "expire after {}. Further rejections are logged at debug level.", kind, id, scrollKeepAlive, exception);
                } else {
                    log.debug("Could not release the Elasticsearch {} {}, which expires after {}", kind, id, scrollKeepAlive, exception);
                }
            }
        });
    }

    /**
     * Whether a status answers the release of a scroll context or a point in time with a rejection which every later
     * release will meet as well:
     * a request the cluster doesn't accept (400, 405) or doesn't permit (401, 403). A context which is gone already
     * (404), a busy cluster (429), a timed out request (408) or a server error are passing, not the release's.
     */
    @VisibleForTesting
    static boolean isRejectedRelease(int statusCode) {
        return statusCode == HttpStatus.SC_BAD_REQUEST || statusCode == HttpStatus.SC_UNAUTHORIZED
            || statusCode == HttpStatus.SC_FORBIDDEN || statusCode == HttpStatus.SC_METHOD_NOT_ALLOWED;
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

    //Reattempts a request which failed transiently, up to retryAttemptLimit times. A status code is transient when
    //it is listed in retryOnErrorCodes; a failure which produced no response at all - and so has no status code -
    //is transient when retryTransportFailures is set. Both are the same definition ElasticSearchIndex classifies
    //the final failure by, so a failure which survives these attempts is handed on rather than contradicted
    private Response performRequestWithRetry(Request request) throws IOException {
        int retryCount = 0;
        while (true) {
            try {
                return delegate.performRequest(request);
            } catch (ResponseException e) {
                if (!retryOnErrorCodes.contains(e.getResponse().getStatusLine().getStatusCode()) || retryCount >= retryAttemptLimit) {
                    throw e;
                }
            } catch (IOException e) {
                if (!retryTransportFailures || !TransientFailures.hasTransportFailureCause(e) || retryCount >= retryAttemptLimit) {
                    throw e;
                }
            }
            performRetryWait(retryCount);
            retryCount++;
        }
    }

    //Anywhere from half of the backoff to all of it, so that requests which failed together, as they do when
    //Elasticsearch is overloaded, don't all come back at the same moment
    @VisibleForTesting
    long retryWaitMs(int retryCount) {
        final long backoffMs = Math.min((long) (retryInitialWaitMs * Math.pow(10, retryCount)), retryMaxWaitMs);
        return backoffMs / 2 + ThreadLocalRandom.current().nextLong(backoffMs - backoffMs / 2 + 1);
    }

    private void performRetryWait(int retryCount) {
        final long waitDurationMs = retryWaitMs(retryCount);
        log.warn("Retrying Elasticsearch request in {} ms. Attempt {} of {}", waitDurationMs, retryCount, retryAttemptLimit);
        try {
            Thread.sleep(waitDurationMs);
        } catch (InterruptedException interruptedException) {
            //Thread.sleep cleared the interrupt status when it threw. Put it back so that whoever is waiting above -
            //BackendOperation, which aborts its own backoff on it - can see the operation was cancelled
            Thread.currentThread().interrupt();
            throw new RuntimeException(String.format("Thread interrupted while waiting for retry attempt %d of %d", retryCount, retryAttemptLimit), interruptedException);
        }
    }

    private Response performRequest(Request request, byte[] requestData) throws IOException {
        return performRequestWithEntity(request,
            requestData != null ? new ByteArrayEntity(requestData, ContentType.APPLICATION_JSON) : null);
    }

    private Response performRequestWithEntity(Request request, HttpEntity entity) throws IOException {

        request.setEntity(entity);

        final Response response = performRequestWithRetry(request);

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
