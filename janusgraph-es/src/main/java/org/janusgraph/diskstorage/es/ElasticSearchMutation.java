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

    private ElasticSearchMutation(RequestType requestType, String index, String type, String id, Map source,
                                  boolean removesContentOnly) {
        this.requestType = requestType;
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
        this.removesContentOnly = removesContentOnly;
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

    public RequestType getRequestType() {
        return requestType;
    }

    public boolean removesContentOnly() {
        return removesContentOnly;
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
