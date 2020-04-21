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

package org.janusgraph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import org.janusgraph.core.Cardinality;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.Mutation;

import java.util.AbstractMap;
import java.util.List;
import java.util.function.Function;

/**
 * An index mutation contains the field updates (additions and deletions) for a particular index entry.
 * In addition it maintains two boolean values: 1) isNew - the entry is newly created, 2) isDeleted -
 * the entire entry is being deleted. These can be used by an {@link IndexProvider} to execute updates more
 * efficiently.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexMutation extends Mutation<IndexEntry,IndexEntry> {

    private final KeyInformation.StoreRetriever storeRetriever;
    private final boolean isNew;
    private boolean isDeleted;

    private final Function<IndexEntry, Object> entryConversionFunction =
            indexEntry -> isCollection(indexEntry.field) ?
                    new AbstractMap.SimpleEntry<>(indexEntry.field, indexEntry.value) :
                    indexEntry.field;

    public IndexMutation(KeyInformation.StoreRetriever storeRetriever,
                         List<IndexEntry> additions, List<IndexEntry> deletions,
                         boolean isNew, boolean isDeleted) {
        super(additions, deletions);
        Preconditions.checkArgument(!(isNew && isDeleted),"Invalid status");
        this.storeRetriever = storeRetriever;
        this.isNew = isNew;
        this.isDeleted = isDeleted;
    }

    public IndexMutation(KeyInformation.StoreRetriever storeRetriever,
                         boolean isNew, boolean isDeleted) {
        super();
        Preconditions.checkArgument(!(isNew && isDeleted),"Invalid status");
        this.storeRetriever = storeRetriever;
        this.isNew = isNew;
        this.isDeleted = isDeleted;
    }

    public void merge(IndexMutation m) {
        Preconditions.checkArgument(isNew == m.isNew,"Incompatible new status");
        Preconditions.checkArgument(isDeleted == m.isDeleted,"Incompatible delete status");
        super.merge(m);
    }

    public boolean isNew() {
        return isNew;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void resetDelete() {
        isDeleted=false;
    }

    private boolean isCollection(String field) {
        KeyInformation keyInformation = storeRetriever.get(field);
        return keyInformation != null && keyInformation.getCardinality() != Cardinality.SINGLE;
    }

    @Override
    public void consolidate() {
        super.consolidate(entryConversionFunction, entryConversionFunction);
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(entryConversionFunction, entryConversionFunction);
    }

    public int determineTTL() {
        return hasDeletions() ? 0 : determineTTL(getAdditions());
    }

    public static int determineTTL(List<IndexEntry> additions) {
        if (additions == null || additions.isEmpty())
            return 0;

        int ttl=-1;
        for (IndexEntry add : additions) {
            int ittl = 0;
            if (add.hasMetaData()) {
                Preconditions.checkArgument(add.getMetaData().size()==1 && add.getMetaData().containsKey(EntryMetaData.TTL),
                        "Index only supports TTL meta data. Found: %s",add.getMetaData());
                ittl = (Integer)add.getMetaData().get(EntryMetaData.TTL);
            }
            if (ttl<0) ttl=ittl;
            Preconditions.checkArgument(ttl==ittl,"Index only supports uniform TTL values across all " +
                    "index fields, but got additions: %s",additions);
        }
        assert ttl>=0;
        return ttl;
    }

}
