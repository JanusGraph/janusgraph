// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.Iterator;

public class JanusGraphLazyVertexProperty<V> extends JanusGraphLazyRelation<V> implements JanusGraphVertexProperty<V> {

    public JanusGraphLazyVertexProperty(InternalRelation janusGraphRelation,
                                  final InternalVertex vertex,
                                  final StandardJanusGraphTx tx,
                                  final InternalRelationType type) {
        super(janusGraphRelation, vertex, tx, type);
    }

    public JanusGraphLazyVertexProperty(Entry dataEntry,
                                  final InternalVertex vertex,
                                  final StandardJanusGraphTx tx,
                                  final InternalRelationType type) {
        super(dataEntry, vertex, tx, type);
    }

    private JanusGraphVertexProperty<V> loadProperty() {
        assert this.isProperty();
        return (JanusGraphVertexProperty<V>) this.loadValue();
    }

    @Override
    public boolean isPresent() {
        return loadProperty().isPresent();
    }

    @Override
    public JanusGraphVertex element() {
        return loadProperty().element();
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return this.loadProperty().properties(propertyKeys);
    }
}
