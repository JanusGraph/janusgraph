package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.LoggableTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Wraps the transaction handle of an index and buffers all mutations against an index for efficiency.
 * Also acts as a proxy to the {@link IndexProvider} methods.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexTransaction implements TransactionHandle, LoggableTransaction {

    private static final int DEFAULT_OUTER_MAP_SIZE = 3;
    private static final int DEFAULT_INNER_MAP_SIZE = 5;

    private final IndexProvider index;
    private final TransactionHandle indexTx;
    private final KeyInformation.IndexRetriever keyInformations;

    private final int mutationAttempts;
    private final int attemptWaitTime;

    private Map<String,Map<String,IndexMutation>> mutations;

    public IndexTransaction(final IndexProvider index, final KeyInformation.IndexRetriever keyInformations,
                            int mutationAttempts, int attemptWaitTime) throws StorageException {
        Preconditions.checkNotNull(index);
        Preconditions.checkNotNull(keyInformations);
        this.index=index;
        this.keyInformations = keyInformations;
        this.indexTx=index.beginTransaction();
        Preconditions.checkNotNull(indexTx);
        this.mutationAttempts = mutationAttempts;
        this.attemptWaitTime = attemptWaitTime;
        this.mutations = new HashMap<String,Map<String,IndexMutation>>(DEFAULT_OUTER_MAP_SIZE);
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
        }
        return m;
    }


    public void register(String store, String key, KeyInformation information) throws StorageException {
        index.register(store,key,information,indexTx);
    }

    public List<String> query(IndexQuery query) throws StorageException {
        return index.query(query,keyInformations,indexTx);
    }

    public Iterable<RawQuery.Result<String>> query(RawQuery query) throws StorageException {
        return index.query(query,keyInformations,indexTx);
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        indexTx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        mutations=null;
        indexTx.rollback();
    }

    @Override
    public void flush() throws StorageException {
        flushInternal();
        indexTx.flush();
    }

    private void flushInternal() throws StorageException {
        if (mutations!=null && !mutations.isEmpty()) {
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
            }, mutationAttempts, attemptWaitTime);

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
                out.putByte((byte)(mut.isNew()?1:0));
                out.putByte((byte)(mut.isDeleted()?1:0));
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
