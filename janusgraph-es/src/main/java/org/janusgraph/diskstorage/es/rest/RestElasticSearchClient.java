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
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.core.JsonParseException;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
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
import org.javatuples.Pair;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    //Set once the first rejected scroll release has been logged as a warning. Every scroll of the index backend is
    //released through this client with the same credentials, so later rejections repeat the same problem and are
    //logged at debug level. Releases complete on the HTTP client's I/O threads, hence the atomic flag
    private final AtomicBoolean warnedAboutRejectedScrollRelease = new AtomicBoolean();

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
        majorVersion = configuredMajorVersion != null ? configuredMajorVersion : getMajorVersion();
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
                mappingTypesRemoved = ElasticMajorVersion.isOpenSearch(info.getVersion());
                if (ElasticMajorVersion.isOpenSearch(info.getVersion())) {
                    log.info("OpenSearch {} provides the Elasticsearch {} API, which JanusGraph uses.",
                        info.getVersion().get("number"), majorVersion.getValue());
                }
            }
        } catch (final IOException e) {
            log.warn("Unable to determine Elasticsearch server version. Default to {}. Set index.[X].elasticsearch.{} to " +
                "skip the detection.", majorVersion, ElasticSearchIndex.MAJOR_VERSION.getName(), e);
        }

        return majorVersion;
    }

    @Override
    public boolean usesMappingTypes() {
        return useMappingTypes;
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
        RequestBytes(final ElasticSearchMutation request) throws JsonProcessingException {
            this.removesContentOnly = request.removesContentOnly();
            this.store = request.getType();
            this.documentId = request.getId();
            Map<String, Object> requestData = new HashMap<>();
            if (useMappingTypes) {
                requestData.put("_index", request.getIndex());
                requestData.put("_type", request.getType());
                requestData.put("_id", request.getId());
            } else {
                requestData.put("_index", request.getIndex());
                requestData.put("_id", request.getId());
            }

            //Elasticsearch's own default is 0, so a request needs the key only above it. Every supported version
            //reads retry_on_conflict; Elasticsearch 6 merely deprecated the _retry_on_conflict it also accepted
            if (retryOnConflict != null && retryOnConflict > 0
                && request.getRequestType() == ElasticSearchMutation.RequestType.UPDATE) {
                requestData.put("retry_on_conflict", retryOnConflict);
            }

            this.requestBytes =  mapWriter.writeValueAsBytes(ImmutableMap.of(request.getRequestType().name().toLowerCase(), requestData));
            if (request.getSource() != null) {
                this.requestSource = mapWriter.writeValueAsBytes(request.getSource());
            } else {
                this.requestSource = null;
            }
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

    private Pair<String, byte[]> buildBulkRequestInput(List<RequestBytes> requests, String ingestPipeline) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (final RequestBytes request : requests) {
            request.writeTo(outputStream);
        }

        final StringBuilder bulkRequestQueryParameters = new StringBuilder();
        if (ingestPipeline != null) {
            APPEND_OP.apply(bulkRequestQueryParameters).append("pipeline=").append(ingestPipeline);
        }
        if (bulkRefreshEnabled) {
            APPEND_OP.apply(bulkRequestQueryParameters).append("refresh=").append(bulkRefresh);
        }
        final String bulkRequestPath = REQUEST_SEPARATOR + "_bulk" + bulkRequestQueryParameters;
        return Pair.with(bulkRequestPath, outputStream.toByteArray());
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
        BulkRequestChunker(List<ElasticSearchMutation> requests) throws JsonProcessingException {
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
        BulkRequestChunker bulkRequestChunker = new BulkRequestChunker(requests);
        while (bulkRequestChunker.hasNext()) {
            List<RequestBytes> bulkRequestChunk = bulkRequestChunker.next();
            int retryCount = 0;
            while (true) {
                final Pair<String, byte[]> bulkRequestInput = buildBulkRequestInput(bulkRequestChunk, ingestPipeline);
                final Response response = performRequest(REQUEST_TYPE_POST, bulkRequestInput.getValue0(), bulkRequestInput.getValue1());
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
        final Request request = new Request(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll");
        //The id goes in the body: the path form has been deprecated since Elasticsearch 7, and an id can outgrow a URL
        request.setEntity(new ByteArrayEntity(
            mapWriter.writeValueAsBytes(ImmutableMap.of("scroll_id", ImmutableList.of(scrollId))), ContentType.APPLICATION_JSON));
        //Releasing the context shouldn't cost the search a round trip, so the request goes out without waiting for
        //its answer. Should it be lost, the context expires after the keep-alive anyway
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
                    && isRejectedScrollRelease(((ResponseException) exception).getResponse().getStatusLine().getStatusCode())
                    && !warnedAboutRejectedScrollRelease.getAndSet(true)) {
                    log.warn("Elasticsearch rejected the release of the scroll {}, so scroll contexts stay open until they " +
                        "expire after {}. Further rejections are logged at debug level.", scrollId, scrollKeepAlive, exception);
                } else {
                    log.debug("Could not release the Elasticsearch scroll {}, which expires after {}", scrollId, scrollKeepAlive, exception);
                }
            }
        });
    }

    /**
     * Whether a status answers the release of a scroll with a rejection which every later release will meet as well:
     * a request the cluster doesn't accept (400, 405) or doesn't permit (401, 403). A context which is gone already
     * (404), a busy cluster (429), a timed out request (408) or a server error are passing, not the release's.
     */
    @VisibleForTesting
    static boolean isRejectedScrollRelease(int statusCode) {
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

        final HttpEntity entity = requestData != null ? new ByteArrayEntity(requestData, ContentType.APPLICATION_JSON) : null;

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
