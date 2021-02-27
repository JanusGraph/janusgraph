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

package org.janusgraph.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;

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
        this.readValues = new HashMap<>();
        this.writtenValues = new HashMap<>();
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
        Iterables.filter(writtenValues.keySet(), s -> s != null && s.startsWith(prefix)),
        Iterables.filter(config.getKeys(prefix), s -> !writtenValues.containsKey(s)));
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
        Preconditions.checkArgument(hasMutations(), "No mutation to log");
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
