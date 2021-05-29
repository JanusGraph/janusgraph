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

package org.janusgraph.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.janusgraph.diskstorage.EntryList;

import java.util.Collections;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionVertexAggregate<M> extends VertexState<M> {

    public PartitionVertexAggregate(Map<MessageScope,Integer> scopeMap) {
        super(Collections.EMPTY_MAP);

    }

    public synchronized void setLoadedProperties(EntryList props) {
        assert properties==null;
        properties = props;
    }

    public EntryList getLoadedProperties() {
        return (EntryList)properties;
    }

    public<V> void setProperty(String key, V value, Map<String,Integer> keyMap) {
        throw new UnsupportedOperationException();
    }

    public<V> V getProperty(String key, Map<String,Integer> keyMap) {
        throw new UnsupportedOperationException();
    }



}
