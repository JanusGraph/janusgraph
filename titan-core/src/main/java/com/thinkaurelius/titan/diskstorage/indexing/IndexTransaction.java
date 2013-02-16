package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: add index transaction
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IndexTransaction implements TransactionHandle {

    private final IndexProvider index;
    private final TransactionHandle indexTx;
    private Map<String,IndexMutation> mutations;

    public IndexTransaction(final IndexProvider index) throws StorageException {
        Preconditions.checkNotNull(index);
        this.index=index;
        this.indexTx=index.beginTransaction();
        Preconditions.checkNotNull(indexTx);
        this.mutations = null;
    }

    public void add(String docid, String key, Object value) {
        getIndexMutation(docid).addition(new IndexEntry(key,value));
    }

    public void delete(String docid, String key) {
        getIndexMutation(docid).deletion(key);
    }

    private IndexMutation getIndexMutation(String docid) {
        if (mutations==null) mutations = new HashMap<String,IndexMutation>();
        IndexMutation m = mutations.get(docid);
        if (m==null) {
            m = new IndexMutation();
            mutations.put(docid,m);
        }
        return m;
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        indexTx.commit();
    }

    @Override
    public void abort() throws StorageException {
        mutations=null;
        indexTx.abort();
    }

    @Override
    public void flush() throws StorageException {
        flushInternal();
        indexTx.flush();
    }

    private void flushInternal() throws StorageException {
        if (mutations!=null && !mutations.isEmpty()) {
            index.mutate(mutations,indexTx);
            mutations=null;
        }
    }
}
