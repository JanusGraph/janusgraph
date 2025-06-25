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
