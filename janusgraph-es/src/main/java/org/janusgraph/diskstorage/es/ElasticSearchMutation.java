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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_UPSERT_KEY;

public class ElasticSearchMutation {

    public enum RequestType {

        INDEX,

        UPDATE,

        DELETE

    }

    private final RequestType requestType;

    private final String index;

    private final String type;

    private final String id;

    private final Map source;

    //A mutation which only takes content out of the index asks for nothing that an absent document does not already
    //satisfy. Elasticsearch reports an absent document as a 404 for such a mutation and for one which puts content in,
    //so the two can only be told apart from the mutation itself
    private final boolean removesContentOnly;

    //The same update without the element's complete document, for when the complete document would make the update
    //too large to send; null for any other mutation
    private final ElasticSearchMutation withoutCompleteDocument;

    private ElasticSearchMutation(RequestType requestType, String index, String type, String id, Map source,
                                  boolean removesContentOnly) {
        this(requestType, index, type, id, source, removesContentOnly, null);
    }

    private ElasticSearchMutation(RequestType requestType, String index, String type, String id, Map source,
                                  boolean removesContentOnly, ElasticSearchMutation withoutCompleteDocument) {
        this.requestType = requestType;
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
        this.removesContentOnly = removesContentOnly;
        this.withoutCompleteDocument = withoutCompleteDocument;
    }

    public static ElasticSearchMutation createDeleteRequest(String index, String type, String id) {
        return new ElasticSearchMutation(RequestType.DELETE, index, type, id, null, true);
    }

    public static ElasticSearchMutation createIndexRequest(String index, String type, String id, Map source) {
        return new ElasticSearchMutation(RequestType.INDEX, index, type, id, source, false);
    }

    //An update which runs a script removing fields from a document, rather than the whole document
    public static ElasticSearchMutation createFieldDeletionRequest(String index, String type, String id, Map source) {
        return new ElasticSearchMutation(RequestType.UPDATE, index, type, id, source, true);
    }

    public static ElasticSearchMutation createUpdateRequest(String index, String type, String id, Map source) {
        return new ElasticSearchMutation(RequestType.UPDATE, index, type, id, source, false);
    }

    public static ElasticSearchMutation createUpdateRequest(String index, String type, String id, ImmutableMap.Builder builder, Map upsert) {
        final Map source = upsert == null ? builder.build() : builder.put(ES_UPSERT_KEY, upsert).build();
        return new ElasticSearchMutation(RequestType.UPDATE, index, type, id, source, false);
    }

    //An update which carries the element's complete document as its upsert, so that a document which turns out to be
    //missing is recreated whole. The complete document only matters in that case, so the same update without it is
    //kept as well: an existing document is updated exactly the same way by it, and it is what is sent when the
    //complete document would make the update too large to send. Without the complete document the update only takes
    //content out of the index when it only removes content
    public static ElasticSearchMutation createUpdateRequestWithCompleteDocument(String index, String type, String id,
                                                                                ImmutableMap.Builder<String, Object> builder,
                                                                                Map<String, Object> completeDocument,
                                                                                boolean removesContentOnlyWithoutIt) {
        final Map<String, Object> update = builder.build();
        final ElasticSearchMutation withoutIt = new ElasticSearchMutation(RequestType.UPDATE, index, type, id, update,
            removesContentOnlyWithoutIt);
        final Map<String, Object> source = ImmutableMap.<String, Object>builder().putAll(update)
            .put(ES_UPSERT_KEY, completeDocument).build();
        return new ElasticSearchMutation(RequestType.UPDATE, index, type, id, source, false, withoutIt);
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public boolean removesContentOnly() {
        return removesContentOnly;
    }

    //The same update without the element's complete document, or null when the mutation carries none
    public ElasticSearchMutation withoutCompleteDocument() {
        return withoutCompleteDocument;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Map getSource() {
        return source;
    }

}
