package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfiguration {

    private static final List<StaticBuffer> NO_DELETIONS = ImmutableList.of();

    private final KeyColumnValueStoreManager manager;
    private final KeyColumnValueStore store;
    private final String identifier;
    private final StaticBuffer rowKey;

    private long maxOperationWaitTime = 10000;


    public KCVSConfiguration(KeyColumnValueStoreManager manager, String configurationName,
                             String identifier ) throws StorageException {
        Preconditions.checkNotNull(manager);
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier));
        Preconditions.checkArgument(StringUtils.isNotBlank(configurationName));
        this.manager = manager;
        this.store = manager.openDatabase(configurationName);
        this.identifier = identifier;
        this.rowKey = string2StaticBuffer(this.identifier);
    }

    public void setMaxOperationWaitTime(long waitTimeMS) {
        Preconditions.checkArgument(waitTimeMS>0,"Invalid wait time: %s",waitTimeMS);
        this.maxOperationWaitTime=waitTimeMS;
    }

    /**
     * Reads the configuration property for this StoreManager
     *
     * @param key Key identifying the configuration property
     * @return Value stored for the key or null if the configuration property has not (yet) been defined.
     * @throws StorageException
     */
    public String getConfigurationProperty(final String key) throws StorageException {
        StaticBuffer column = string2StaticBuffer(key);
        final KeySliceQuery query = new KeySliceQuery(rowKey,column, ByteBufferUtil.nextBiggerBuffer(column),false);

        StaticBuffer result = BackendOperation.execute(new Callable<StaticBuffer>() {
            @Override
            public StaticBuffer call() throws Exception {
                StoreTransaction txh = null;
                try {
                    txh = manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT));
                    List<Entry> entries = store.getSlice(query,txh);
                    if (entries.isEmpty()) return null;
                    return entries.get(0).getValue();
                } finally {
                    if (txh != null) txh.commit();
                }
            }

            @Override
            public String toString() {
                return "getConfiguration";
            }
        }, maxOperationWaitTime);
        if (result==null) return null;
        return staticBuffer2String(result);
    }

    /**
     * Sets a configuration property for this StoreManager.
     *
     * @param key   Key identifying the configuration property
     * @param value Value to be stored for the key
     * @throws StorageException
     */
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        StaticBuffer column = string2StaticBuffer(key);
        StaticBuffer val = string2StaticBuffer(value);
        final List<Entry> additions = new ArrayList<Entry>(1);
        additions.add(new StaticBufferEntry(column,val));

        BackendOperation.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                StoreTransaction txh=null;
                try {
                    txh= manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT));
                    store.mutate(rowKey, additions, NO_DELETIONS,txh);
                    return true;
                } finally {
                    if (txh!=null) txh.commit();
                }
            }

            @Override
            public String toString() {
                return "setConfiguration";
            }
        },maxOperationWaitTime);
    }



    public void close() throws StorageException {
        store.close();
    }

    private StaticBuffer string2StaticBuffer(final String s) {
        ByteBuffer out = ByteBuffer.wrap(s.getBytes(Charset.forName("UTF-8")));
        return new StaticByteBuffer(out);
    }

    private String staticBuffer2String(final StaticBuffer s) {
        return new String(s.as(StaticBuffer.ARRAY_FACTORY),Charset.forName("UTF-8"));
    }

}
