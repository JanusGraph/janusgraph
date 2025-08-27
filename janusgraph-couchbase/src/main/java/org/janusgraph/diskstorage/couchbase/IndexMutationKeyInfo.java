/*
 * Copyright 2025 Couchbase, Inc.
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

import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.KeyInformation;

public class IndexMutationKeyInfo {
    private final IndexMutation mutation;
    private final KeyInformation.IndexRetriever keyInformation;

    public IndexMutationKeyInfo(IndexMutation mutation, KeyInformation.IndexRetriever keyInformation) {
        this.mutation = mutation;
        this.keyInformation = keyInformation;
    }

    public IndexMutation mutation() {
        return mutation;
    }

    public KeyInformation.IndexRetriever keyInformation() {
        return keyInformation;
    }
}
