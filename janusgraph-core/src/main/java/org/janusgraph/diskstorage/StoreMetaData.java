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

package org.janusgraph.diskstorage;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Store-specific (Column-family-specific) options passed between
 * JanusGraph core and its underlying KeyColumnValueStore implementation.
 * This is part of JanusGraph's internals and is not user-facing in
 * ordinary operation.
 */
public interface StoreMetaData<T> {

    Class<? extends T> getDataType();

    StoreMetaData<Integer> TTL = TTLImpl.INSTANCE;

    Container EMPTY = new Container(false);

    class Container {

        private final boolean mutable;

        private final Map<StoreMetaData<?>, Object> md = new HashMap<>();

        public Container() {
            this(true);
        }

        public Container(boolean mutable) {
            this.mutable = mutable;
        }

        public <V, K extends StoreMetaData<V>> void put(K type, V value) {
            Preconditions.checkState(mutable);
            md.put(type, value);
        }

        public <V, K extends StoreMetaData<V>> V get(K type) {
            return type.getDataType().cast(md.get(type));
        }

        public <K extends StoreMetaData<?>> boolean contains(K type) {
            return md.containsKey(type);
        }

        public int size() {
            return md.size();
        }

        public boolean isEmpty() {
            return md.isEmpty();
        }
    }

    /**
     * Time-to-live for all data written to the store.  Values associated
     * with this enum will be expressed in seconds.  The TTL is only required
     * to be honored when the associated store is opened for the first time.
     * Subsequent re-openings of an existing store need not check for or
     * modify the existing TTL (though implementations are free to do so).
     */
    enum TTLImpl implements StoreMetaData<Integer> {
        INSTANCE;

        @Override
        public Class<? extends Integer> getDataType() {
            return Integer.class;
        }
    }
}
