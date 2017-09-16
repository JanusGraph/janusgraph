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

import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.util.StreamIterable;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wraps the transaction handle of an index and buffers all mutations against an index for efficiency.
 * Also acts as a proxy to the {@link IndexProvider} methods.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexTransaction implements BaseTransaction, LoggableTransaction {

    private static final int DEFAULT_OUTER_MAP_SIZE = 3;
    private static final int DEFAULT_INNER_MAP_SIZE = 5;

    private final IndexProvider index;
    private final BaseTransaction indexTx;
    private final KeyInformation.IndexRetriever keyInformations;

    private final Duration maxWriteTime;

    private Map<String,Map<String,IndexMutation>> mutations;

    public IndexTransaction(final IndexProvider index, final KeyInformation.IndexRetriever keyInformations,
                            BaseTransactionConfig config,
                            Duration maxWriteTime) throws BackendException {
        Preconditions.checkNotNull(index);
        Preconditions.checkNotNull(keyInformations);
        this.index=index;
        this.keyInformations = keyInformations;
        this.indexTx=index.beginTransaction(config);
        Preconditions.checkNotNull(indexTx);
        this.maxWriteTime = maxWriteTime;
        this.mutations = new HashMap<String,Map<String,IndexMutation>>(DEFAULT_OUTER_MAP_SIZE);
    }

    public void add(String store, String docid, IndexEntry entry, boolean isNew) {
        getIndexMutation(store,docid, isNew, false).addition(new IndexEntry(entry.field, entry.value, entry.getMetaData()));
    }

    public void add(String store, String docid, String key, Object value, boolean isNew) {
        getIndexMutation(store,docid,isNew,false).addition(new IndexEntry(key,value));
    }

    public void delete(String store, String docid, String key, Object value, boolean deleteAll) {
        getIndexMutation(store,docid,false,deleteAll).deletion(new IndexEntry(key,value));
    }

    private IndexMutation getIndexMutation(String store, String docid, boolean isNew, boolean isDeleted) {
        Map<String,IndexMutation> storeMutations = mutations.get(store);
        if (storeMutations==null) {
            storeMutations = new HashMap<String,IndexMutation>(DEFAULT_INNER_MAP_SIZE);
            mutations.put(store,storeMutations);

        }
        IndexMutation m = storeMutations.get(docid);
        if (m==null) {
            m = new IndexMutation(isNew,isDeleted);
            storeMutations.put(docid, m);
        } else {
            //IndexMutation already exists => if we deleted and re-created it we need to remove the deleted flag
            if (isNew && m.isDeleted()) {
                m.resetDelete();
                assert !m.isNew() && !m.isDeleted();
            }
        }
        return m;
    }


    public void register(String store, String key, KeyInformation information) throws BackendException {
        index.register(store,key,information,indexTx);
    }

    /**
     * @deprecated use {@link #queryStream(IndexQuery query)} instead.
     */
    @Deprecated
    public List<String> query(IndexQuery query) throws BackendException {
        return queryStream(query).collect(Collectors.toList());
    }

    public Stream<String> queryStream(IndexQuery query) throws BackendException {
        return index.query(query,keyInformations,indexTx);
    }

    /**
     * @deprecated use {@link #queryStream(RawQuery query)} instead.
     */
    @Deprecated
    public Iterable<RawQuery.Result<String>> query(RawQuery query) throws BackendException {
        return new StreamIterable<>(index.query(query,keyInformations,indexTx));
    }

    public Stream<RawQuery.Result<String>> queryStream(RawQuery query) throws BackendException {
        return index.query(query,keyInformations,indexTx);
    }

    public Long totals(RawQuery query) throws BackendException {
        return index.totals(query,keyInformations,indexTx);
    }

    public void restore(Map<String, Map<String,List<IndexEntry>>> documents) throws BackendException {
        index.restore(documents,keyInformations,indexTx);
    }

    @Override
    public void commit() throws BackendException {
        flushInternal();
        indexTx.commit();
    }

    @Override
    public void rollback() throws BackendException {
        mutations=null;
        indexTx.rollback();
    }



    private void flushInternal() throws BackendException {
        if (mutations!=null && !mutations.isEmpty()) {
            //Consolidate all mutations prior to persistence to ensure that no addition accidentally gets swallowed by a delete
            for (Map<String, IndexMutation> store : mutations.values()) {
                for (IndexMutation mut : store.values()) mut.consolidate();
            }

            BackendOperation.execute(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    index.mutate(mutations, keyInformations, indexTx);
                    return true;
                }

                @Override
                public String toString() {
                    return "IndexMutation";
                }
            }, maxWriteTime);

            mutations=null;
        }
    }

    @Override
    public void logMutations(DataOutput out) {
        VariableLong.writePositive(out,mutations.size());
        for (Map.Entry<String,Map<String,IndexMutation>> store : mutations.entrySet()) {
            out.writeObjectNotNull(store.getKey());
            VariableLong.writePositive(out,store.getValue().size());
            for (Map.Entry<String,IndexMutation> doc : store.getValue().entrySet()) {
                out.writeObjectNotNull(doc.getKey());
                IndexMutation mut = doc.getValue();
                out.putByte((byte)(mut.isNew()?1:(mut.isDeleted()?2:0)));
                List<IndexEntry> adds = mut.getAdditions();
                VariableLong.writePositive(out,adds.size());
                for (IndexEntry add : adds) writeIndexEntry(out,add);
                List<IndexEntry> dels = mut.getDeletions();
                VariableLong.writePositive(out,dels.size());
                for (IndexEntry del: dels) writeIndexEntry(out,del);
            }
        }
    }

    private void writeIndexEntry(DataOutput out, IndexEntry entry) {
        out.writeObjectNotNull(entry.field);
        out.writeClassAndObject(entry.value);
    }

}
