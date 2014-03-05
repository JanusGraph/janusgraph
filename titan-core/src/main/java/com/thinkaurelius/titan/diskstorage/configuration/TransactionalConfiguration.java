package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TransactionalConfiguration implements WriteConfiguration {

    private final WriteConfiguration config;

    private final Map<String,Object> readValues;
    private final Map<String,Object> writtenValues;

    public TransactionalConfiguration(WriteConfiguration config) {
        Preconditions.checkNotNull(config);
        this.config = config;
        this.readValues = new HashMap<String, Object>();
        this.writtenValues = new HashMap<String, Object>();
    }

    @Override
    public <O> void set(String key, O value) {
        writtenValues.put(key,value);
    }

    @Override
    public void remove(String key) {
        writtenValues.put(key,null);
    }

    @Override
    public WriteConfiguration copy() {
        return config.copy();
    }

    @Override
    public <O> O get(String key, Class<O> datatype) {
        Object value = writtenValues.get(key);
        if (value!=null) return (O)value;
        value = readValues.get(key);
        if (value!=null) return (O)value;
        value = config.get(key,datatype);
        readValues.put(key,value);
        return (O)value;
    }

    @Override
    public Iterable<String> getKeys(final String prefix) {
        return Iterables.concat(
        Iterables.filter(writtenValues.keySet(),new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String s) {
                return s.startsWith(prefix);
            }
        }),
        Iterables.filter(config.getKeys(prefix),new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String s) {
                return !writtenValues.containsKey(s);
            }
        }));
    }

    public void commit() {
        for (Map.Entry<String,Object> entry : writtenValues.entrySet()) {
            if (config instanceof ConcurrentWriteConfiguration && readValues.containsKey(entry.getKey())) {
                ((ConcurrentWriteConfiguration)config)
                        .set(entry.getKey(), entry.getValue(), readValues.get(entry.getKey()));
            } else {
                config.set(entry.getKey(),entry.getValue());
            }
        }
        rollback();
    }

    public void rollback() {
        writtenValues.clear();
        readValues.clear();
    }

    public boolean hasMutations() {
        return !writtenValues.isEmpty();
    }

    public void logMutations(DataOutput out) {
        Preconditions.checkArgument(hasMutations());
        VariableLong.writePositive(out,writtenValues.size());
        for (Map.Entry<String,Object> entry : writtenValues.entrySet()) {
            out.writeObjectNotNull(entry.getKey());
            out.writeClassAndObject(entry.getValue());
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
