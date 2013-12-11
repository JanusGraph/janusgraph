package com.thinkaurelius.titan.diskstorage.configuration.backend;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfiguration implements WriteConfiguration {

    private static final List<StaticBuffer> NO_DELETIONS = ImmutableList.of();

    private final KeyColumnValueStoreManager manager;
    private final KeyColumnValueStore store;
    private final String identifier;
    private final StaticBuffer rowKey;

    private final Cache<String,Holder> cache;
    private final KryoSerializer serializer;

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

        this.serializer = new KryoSerializer(true);
        this.cache = CacheBuilder.newBuilder().concurrencyLevel(1).initialCapacity(96).maximumSize(2048).build();
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
    @Override
    public <O> O get(final String key, final Class<O> datatype) {
        StaticBuffer column = string2StaticBuffer(key);
        final KeySliceQuery query = new KeySliceQuery(rowKey,column, ByteBufferUtil.nextBiggerBuffer(column),false);

        try {
            return (O)cache.get(key,new Callable<Holder>() {
                @Override
                public Holder call() throws Exception {
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
                    if (result==null) return new Holder(null);
                    return new Holder(staticBuffer2Object(result, datatype));
                }
            }).get();
        } catch (ExecutionException e) {
            throw new TitanException(e);
        }
    }

    /**
     * Sets a configuration property for this StoreManager.
     *
     * @param key   Key identifying the configuration property
     * @param value Value to be stored for the key
     * @throws StorageException
     */
    @Override
    public <O> void set(String key, O value) {
        StaticBuffer column = string2StaticBuffer(key);
        StaticBuffer val = object2StaticBuffer(value);
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
        cache.put(key,new Holder(value));
    }


    @Override
    public Iterable<String> getKeys(String prefix) {

        List<Entry> result = BackendOperation.execute(new Callable<List<Entry>>() {
            @Override
            public List<Entry> call() throws Exception {
                StoreTransaction txh=null;
                try {
                    txh= manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT));
                    return store.getSlice(new KeySliceQuery(rowKey,ByteBufferUtil.zeroBuffer(128),ByteBufferUtil.oneBuffer(128)),txh);
                } finally {
                    if (txh!=null) txh.commit();
                }
            }

            @Override
            public String toString() {
                return "setConfiguration";
            }
        },maxOperationWaitTime);

        List<String> keys = Lists.newArrayList();
        for (Entry entry : result) {
            String value = staticBuffer2String(entry.getColumn());
            if (value.startsWith(prefix)) keys.add(value);
        }
        return keys;
    }


    @Override
    public void close() {
        try {
            store.close();
            cache.invalidateAll();
        } catch (StorageException e) {
            throw new TitanException("Could not close configuration store",e);
        }
    }

    private StaticBuffer string2StaticBuffer(final String s) {
        ByteBuffer out = ByteBuffer.wrap(s.getBytes(Charset.forName("UTF-8")));
        return new StaticByteBuffer(out);
    }

    private String staticBuffer2String(final StaticBuffer s) {
        return new String(s.as(StaticBuffer.ARRAY_FACTORY),Charset.forName("UTF-8"));
    }

    private<O> StaticBuffer object2StaticBuffer(final O value) {
        DataOutput out = serializer.getDataOutput(128, true);
        out.writeClassAndObject(value);
        return out.getStaticBuffer();
    }

    private<O> O staticBuffer2Object(final StaticBuffer s, Class<O> datatype) {
        Object value = serializer.readClassAndObject(s.asReadBuffer());
        Preconditions.checkArgument(datatype.isInstance(value),"Could not deserialize to [%s], got: %s",datatype,value);
        return (O)value;
    }

    private static class Holder {

        final Object value;

        private Holder(Object value) {
            this.value = value;
        }

        public Object get() {
            return value;
        }

    }


}
