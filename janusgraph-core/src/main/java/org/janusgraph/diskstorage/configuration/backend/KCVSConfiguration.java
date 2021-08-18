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

package org.janusgraph.diskstorage.configuration.backend;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ConcurrentWriteConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.util.datastructures.ExceptionWrapper;
import org.janusgraph.util.system.IOUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static org.janusgraph.util.system.ExecuteUtil.executeWithCatching;
import static org.janusgraph.util.system.ExecuteUtil.throwIfException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfiguration implements ConcurrentWriteConfiguration {

    private final BackendOperation.TransactionalProvider txProvider;
    private final TimestampProvider times;
    private final KeyColumnValueStore store;
    private final StaticBuffer rowKey;
    private final StandardSerializer serializer;

    private Duration maxOperationWaitTime = Duration.ofMillis(10000L);

    public KCVSConfiguration(BackendOperation.TransactionalProvider txProvider, Configuration config,
                             KeyColumnValueStore store, String identifier) throws BackendException {
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier));
        this.txProvider = Preconditions.checkNotNull(txProvider);
        this.times = config.get(TIMESTAMP_PROVIDER);
        this.store = Preconditions.checkNotNull(store);
        this.rowKey = string2StaticBuffer(identifier);
        this.serializer = new StandardSerializer();
    }

    public void setMaxOperationWaitTime(Duration waitTime) {
        Preconditions.checkArgument(Duration.ZERO.compareTo(waitTime) < 0,
                "Wait time must be nonnegative: %s", waitTime);
        this.maxOperationWaitTime = waitTime;
    }



    /**
     * Reads the configuration property for this StoreManager
     *
     * @param key Key identifying the configuration property
     * @return Value stored for the key or null if the configuration property has not (yet) been defined.
     */
    @Override
    public <O> O get(final String key, final Class<O> dataType) {
        StaticBuffer column = string2StaticBuffer(key);
        final KeySliceQuery query = new KeySliceQuery(rowKey,column, BufferUtil.nextBiggerBuffer(column));
        StaticBuffer result = BackendOperation.execute(new BackendOperation.Transactional<StaticBuffer>() {
            @Override
            public StaticBuffer call(StoreTransaction txh) throws BackendException {
                List<Entry> entries = store.getSlice(query,txh);
                if (entries.isEmpty()) return null;
                return entries.get(0).getValueAs(StaticBuffer.STATIC_FACTORY);
            }

            @Override
            public String toString() {
                return "getConfiguration";
            }
        }, txProvider, times, maxOperationWaitTime);
        if (result==null) return null;
        return staticBuffer2Object(result, dataType);
    }

    public<O> void set(String key, O value, O expectedValue) {
        set(key,value,expectedValue,true);
    }

    /**
     * Sets a configuration property for this StoreManager.
     *
     * @param key   Key identifying the configuration property
     * @param value Value to be stored for the key
     */
    @Override
    public <O> void set(String key, O value) {
        set(key,value,null,false);
    }

    public <O> void set(String key, O value, O expectedValue, final boolean checkExpectedValue) {
        final StaticBuffer column = string2StaticBuffer(key);
        final List<Entry> additions;
        final List<StaticBuffer> deletions;
        if (value!=null) { //Addition
            additions = new ArrayList<>(1);
            deletions = KeyColumnValueStore.NO_DELETIONS;
            StaticBuffer val = object2StaticBuffer(value);
            additions.add(StaticArrayEntry.of(column, val));
        } else { //Deletion
            additions = KeyColumnValueStore.NO_ADDITIONS;
            deletions = new ArrayList<>(1);
            deletions.add(column);
        }
        final StaticBuffer expectedValueBuffer;
        if (checkExpectedValue && expectedValue!=null) {
            expectedValueBuffer = object2StaticBuffer(expectedValue);
        } else {
            expectedValueBuffer = null;
        }

        BackendOperation.execute(new BackendOperation.Transactional<Boolean>() {
            @Override
            public Boolean call(StoreTransaction txh) throws BackendException {
                if (checkExpectedValue)
                    store.acquireLock(rowKey,column,expectedValueBuffer,txh);
                store.mutate(rowKey, additions, deletions, txh);
                return true;
            }

            @Override
            public String toString() {
                return "setConfiguration";
            }
        }, txProvider, times, maxOperationWaitTime);
    }

    @Override
    public void remove(String key) {
        set(key,null);
    }

    @Override
    public WriteConfiguration copy() {
        throw new UnsupportedOperationException();
    }

    private Map<String,Object> toMap() {
        List<Entry> result = BackendOperation.execute(new BackendOperation.Transactional<List<Entry>>() {
            @Override
            public List<Entry> call(StoreTransaction txh) throws BackendException {
                return store.getSlice(new KeySliceQuery(rowKey, BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)),txh);
            }

            @Override
            public String toString() {
                return "setConfiguration";
            }
        },txProvider, times, maxOperationWaitTime);

        Map<String,Object> entries = new HashMap<>(result.size());
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
            public <O> O get(String key, Class<O> dataType) {
                Preconditions.checkArgument(!entries.containsKey(key) || dataType.isAssignableFrom(entries.get(key).getClass()));
                return (O)entries.get(key);
            }

            @Override
            public Iterable<String> getKeys(final String prefix) {
                final boolean prefixBlank = StringUtils.isBlank(prefix);
                return entries.keySet().stream().filter(s -> prefixBlank || s.startsWith(prefix)).collect(Collectors.toList());
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
            ExceptionWrapper exceptionWrapper = new ExceptionWrapper();
            executeWithCatching(store::close, exceptionWrapper);
            executeWithCatching(txProvider::close, exceptionWrapper);
            IOUtils.closeQuietly(serializer);
            throwIfException(exceptionWrapper);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not close configuration store",e);
        }
    }

    private StaticBuffer string2StaticBuffer(final String s) {
        ByteBuffer out = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
        return StaticArrayBuffer.of(out);
    }

    private String staticBuffer2String(final StaticBuffer s) {
        return new String(s.as(StaticBuffer.ARRAY_FACTORY), StandardCharsets.UTF_8);
    }

    private<O> StaticBuffer object2StaticBuffer(final O value) {
        if (value==null) throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        if (!serializer.validDataType(value.getClass())) throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);
        DataOutput out = serializer.getDataOutput(128);
        out.writeClassAndObject(value);
        return out.getStaticBuffer();
    }

    private<O> O staticBuffer2Object(final StaticBuffer s, Class<O> dataType) {
        Object value = serializer.readClassAndObject(s.asReadBuffer());
        Preconditions.checkArgument(dataType.isInstance(value),"Could not deserialize to [%s], got: %s",dataType,value);
        return (O)value;
    }

}
