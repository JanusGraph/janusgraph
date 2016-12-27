package com.thinkaurelius.titan.diskstorage.keycolumnvalue.ttl;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVSProxy;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TTLKCVS extends KCVSProxy {

    private final int ttl;

    public TTLKCVS(KeyColumnValueStore store, int ttl) {
        super(store);
        this.ttl = ttl;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        TTLKCVSManager.applyTTL(additions, ttl);
        store.mutate(key, additions, deletions, unwrapTx(txh));
    }

}
