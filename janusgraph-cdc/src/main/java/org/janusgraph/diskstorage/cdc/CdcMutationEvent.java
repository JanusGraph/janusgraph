// Copyright 2025 JanusGraph Authors
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

package org.janusgraph.diskstorage.cdc;

import org.janusgraph.diskstorage.indexing.IndexEntry;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Represents a CDC event for a mixed index mutation.
 * This event captures the changes made to a specific document in a mixed index store.
 */
public class CdcMutationEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String storeName;
    private final String documentId;
    private final List<IndexEntry> additions;
    private final List<IndexEntry> deletions;
    private final boolean isNew;
    private final boolean isDeleted;
    private final long timestamp;
    private final MutationType mutationType;

    public enum MutationType {
        ADDED,
        UPDATED,
        DELETED
    }

    public CdcMutationEvent(String storeName, String documentId,
                           List<IndexEntry> additions, List<IndexEntry> deletions,
                           boolean isNew, boolean isDeleted,
                           long timestamp) {
        this.storeName = storeName;
        this.documentId = documentId;
        this.additions = additions;
        this.deletions = deletions;
        this.isNew = isNew;
        this.isDeleted = isDeleted;
        this.timestamp = timestamp;
        this.mutationType = determineMutationType(isNew, isDeleted, additions, deletions);
    }

    private static MutationType determineMutationType(boolean isNew, boolean isDeleted,
                                                      List<IndexEntry> additions, List<IndexEntry> deletions) {
        if (isDeleted) {
            return MutationType.DELETED;
        } else if (isNew) {
            return MutationType.ADDED;
        } else {
            return MutationType.UPDATED;
        }
    }

    public String getStoreName() {
        return storeName;
    }

    public String getDocumentId() {
        return documentId;
    }

    public List<IndexEntry> getAdditions() {
        return additions;
    }

    public List<IndexEntry> getDeletions() {
        return deletions;
    }

    public boolean isNew() {
        return isNew;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public MutationType getMutationType() {
        return mutationType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CdcMutationEvent that = (CdcMutationEvent) o;
        return isNew == that.isNew &&
                isDeleted == that.isDeleted &&
                timestamp == that.timestamp &&
                Objects.equals(storeName, that.storeName) &&
                Objects.equals(documentId, that.documentId) &&
                Objects.equals(additions, that.additions) &&
                Objects.equals(deletions, that.deletions) &&
                mutationType == that.mutationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(storeName, documentId, additions, deletions, isNew, isDeleted, timestamp, mutationType);
    }

    @Override
    public String toString() {
        return "CdcMutationEvent{" +
                "storeName='" + storeName + '\'' +
                ", documentId='" + documentId + '\'' +
                ", mutationType=" + mutationType +
                ", additions=" + (additions != null ? additions.size() : 0) +
                ", deletions=" + (deletions != null ? deletions.size() : 0) +
                ", isNew=" + isNew +
                ", isDeleted=" + isDeleted +
                ", timestamp=" + timestamp +
                '}';
    }
}
