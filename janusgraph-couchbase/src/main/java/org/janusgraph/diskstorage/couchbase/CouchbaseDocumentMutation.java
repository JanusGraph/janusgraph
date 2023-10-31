/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;

public class CouchbaseDocumentMutation {
    private String table;
    private String documentId;
    private KCVMutation mutation;

    public CouchbaseDocumentMutation(String table, String documentId, KCVMutation mutation) {
        this.table = table;
        this.documentId = documentId;
        this.mutation = mutation;
    }

    public String getTable() {
        return table;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getHashId() {
        return CouchbaseColumnConverter.toId(documentId);
    }

    public KCVMutation getMutation() {
        return mutation;
    }

    public String getDocumentKey() {
        return documentId;
    }
}
