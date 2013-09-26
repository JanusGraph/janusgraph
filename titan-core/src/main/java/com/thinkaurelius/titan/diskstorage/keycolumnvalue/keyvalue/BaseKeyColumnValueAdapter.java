package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class BaseKeyColumnValueAdapter implements KeyColumnValueStore {

    private final KeyValueStore store;

    public BaseKeyColumnValueAdapter(KeyValueStore store) {
        Preconditions.checkNotNull(store);
        this.store = store;
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        List<List<Entry>> resultSet = new ArrayList<List<Entry>>(keys.size());
        for (StaticBuffer key : keys) {
            resultSet.add(getSlice(new KeySliceQuery(key, query), txh));
        }
        return resultSet;
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        return store.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void close() throws StorageException {
        store.close();
    }


}
