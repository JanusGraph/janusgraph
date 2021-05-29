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

package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleIndexCache implements IndexCache {

    private final HashMultimap<Object,JanusGraphVertexProperty> map;

    public SimpleIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public void add(JanusGraphVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public void remove(JanusGraphVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> get(final Object value, final PropertyKey key) {
        return Iterables.filter(map.get(value), janusgraphProperty -> janusgraphProperty.propertyKey().equals(key));
    }

    @Override
    public void close() {
        map.clear();
    }
}
