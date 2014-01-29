package com.thinkaurelius.titan.diskstorage.configuration.backend;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.ReadConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfiguration implements WriteConfiguration {

    private final KeyColumnValueStoreManager manager;
    private final KeyColumnValueStore store;
    private final String identifier;
    private final StaticBuffer rowKey;

    private final StandardSerializer serializer;

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

        this.serializer = new StandardSerializer();
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
        final KeySliceQuery query = new KeySliceQuery(rowKey,column, ByteBufferUtil.nextBiggerBuffer(column));
        StaticBuffer result = BackendOperation.execute(new Callable<StaticBuffer>() {
            @Override
            public StaticBuffer call() throws Exception {
                StoreTransaction txh = null;
                try {
                    txh = manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT));
                    List<Entry> entries = store.getSlice(query,txh);
                    if (entries.isEmpty()) return null;
                    return entries.get(0).getValueAs(StaticBuffer.STATIC_FACTORY);
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
        return staticBuffer2Object(result, datatype);
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
        additions.add(StaticArrayEntry.of(column, val));

        BackendOperation.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                StoreTransaction txh = null;
                try {
                    txh = manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT));
                    store.mutate(rowKey, additions, KeyColumnValueStore.NO_DELETIONS, txh);
                    return true;
                } finally {
                    if (txh != null) txh.commit();
                }
            }

            @Override
            public String toString() {
                return "setConfiguration";
            }
        }, maxOperationWaitTime);
    }

    @Override
    public void remove(String key) {
        if (get(key,Object.class)!=null) {
            StaticBuffer column = string2StaticBuffer(key);
            final List<StaticBuffer> deletions = Lists.newArrayList(column);

            BackendOperation.execute(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    StoreTransaction txh = null;
                    try {
                        txh = manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT));
                        store.mutate(rowKey, KeyColumnValueStore.NO_ADDITIONS, deletions, txh);
                        return true;
                    } finally {
                        if (txh != null) txh.commit();
                    }
                }

                @Override
                public String toString() {
                    return "setConfiguration";
                }
            }, maxOperationWaitTime);
        }
    }

    @Override
    public WriteConfiguration clone() {
        throw new UnsupportedOperationException();
    }

    private Map<String,Object> toMap() {
        Map<String,Object> entries = Maps.newHashMap();
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

        for (Entry entry : result) {
            String key = staticBuffer2String(entry.getColumnAs(StaticBuffer.STATIC_FACTORY));
            Object value = staticBuffer2Object(entry.getValueAs(StaticBuffer.STATIC_FACTORY), Object.class);
            entries.put(key,value);
        }
        return entries;
    }

    public ReadConfiguration asReadConfiguration() {
        final Map<String,Object> entries = toMap();
        return new ReadConfiguration() {
            @Override
            public <O> O get(String key, Class<O> datatype) {
                Preconditions.checkArgument(!entries.containsKey(key) || datatype.isAssignableFrom(entries.get(key).getClass()));
                return (O)entries.get(key);
            }

            @Override
            public Iterable<String> getKeys(final String prefix) {
                return Lists.newArrayList(Iterables.filter(entries.keySet(),new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String s) {
                        assert s!=null;
                        return StringUtils.isBlank(prefix) || s.startsWith(prefix);
                    }
                }));
            }

            @Override
            public void close() {
                //Do nothing
            }
        };
    }

    @Override
    public Iterable<String> getKeys(String prefix) {
        return asReadConfiguration().getKeys(prefix);
    }


    @Override
    public void close() {
        try {
            store.close();
        } catch (StorageException e) {
            throw new TitanException("Could not close configuration store",e);
        }
    }

    private StaticBuffer string2StaticBuffer(final String s) {
        ByteBuffer out = ByteBuffer.wrap(s.getBytes(Charset.forName("UTF-8")));
        return StaticArrayBuffer.of(out);
    }

    private String staticBuffer2String(final StaticBuffer s) {
        return new String(s.as(StaticBuffer.ARRAY_FACTORY),Charset.forName("UTF-8"));
    }

    private<O> StaticBuffer object2StaticBuffer(final O value) {
        DataOutput out = serializer.getDataOutput(128);
        out.writeClassAndObject(value);
        return out.getStaticBuffer();
    }

    private<O> O staticBuffer2Object(final StaticBuffer s, Class<O> datatype) {
        Object value = serializer.readClassAndObject(s.asReadBuffer());
        Preconditions.checkArgument(datatype.isInstance(value),"Could not deserialize to [%s], got: %s",datatype,value);
        return (O)value;
    }


}
