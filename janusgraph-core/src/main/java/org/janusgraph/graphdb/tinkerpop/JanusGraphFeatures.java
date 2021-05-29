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

package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

/**
 * Blueprint's features of a JanusGraph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class JanusGraphFeatures implements Graph.Features {

    private final GraphFeatures graphFeatures;
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures;

    private final StandardJanusGraph graph;

    private JanusGraphFeatures(StandardJanusGraph graph, StoreFeatures storageFeatures) {
        graphFeatures = new JanusGraphGeneralFeatures(storageFeatures.supportsPersistence());
        vertexFeatures = new JanusGraphVertexFeatures();
        edgeFeatures = new JanusGraphEdgeFeatures();
        this.graph = graph;
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public static JanusGraphFeatures getFeatures(StandardJanusGraph graph, StoreFeatures storageFeatures) {
        return new JanusGraphFeatures(graph,storageFeatures);
    }

    private static class JanusGraphDataTypeFeatures implements DataTypeFeatures {

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }

    private static class JanusGraphVariableFeatures extends JanusGraphDataTypeFeatures implements VariableFeatures { }

    private static class JanusGraphGeneralFeatures extends JanusGraphDataTypeFeatures implements GraphFeatures {

        private final boolean persists;

        private JanusGraphGeneralFeatures(boolean persists) {
            this.persists = persists;
        }

        @Override
        public VariableFeatures variables() {
            return new JanusGraphVariableFeatures();
        }

        @Override
        public boolean supportsComputer() {
            return true;
        }

        @Override
        public boolean supportsPersistence() {
            return persists;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return true;
        }
    }

    private static class JanusGraphVertexPropertyFeatures extends JanusGraphDataTypeFeatures implements VertexPropertyFeatures {

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() { return false; }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }
    }

    private static class JanusGraphEdgePropertyFeatures extends JanusGraphDataTypeFeatures implements EdgePropertyFeatures {

    }

    private class JanusGraphVertexFeatures implements VertexFeatures {

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            StandardJanusGraphTx tx = (StandardJanusGraphTx)JanusGraphFeatures.this.graph.newTransaction();
            try {
                if (!tx.containsPropertyKey(key)) return tx.getConfiguration().getAutoSchemaMaker().defaultPropertyCardinality(key).convert();
                return tx.getPropertyKey(key).cardinality().convert();
            } finally {
                tx.rollback();
            }
        }

        @Override
        public VertexPropertyFeatures properties() {
            return new JanusGraphVertexPropertyFeatures();
        }

        @Override
        public boolean supportsNumericIds()
        {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return JanusGraphFeatures.this.graph.getConfiguration().allowVertexIdSetting();
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds()
        {
            return false;
        }

        @Override
        public boolean supportsCustomIds()
        {
            return false;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }
    }

    private static class JanusGraphEdgeFeatures implements EdgeFeatures {
        @Override
        public EdgePropertyFeatures properties() {
            return new JanusGraphEdgePropertyFeatures();
        }

        @Override
        public boolean supportsCustomIds()
        {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() { return false; }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds()
        {
            return false;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }
    }

}
