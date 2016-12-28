package org.janusgraph.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.*;

import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class BaseKeyColumnValueAdapter implements KeyColumnValueStore {

    private final KeyValueStore store;
    private boolean isClosed = false;

    public BaseKeyColumnValueAdapter(KeyValueStore store) {
        Preconditions.checkNotNull(store);
        this.store = store;
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void close() throws BackendException {
        store.close();
        isClosed=true;
    }

    public boolean isClosed() {
        return isClosed;
    }


}
