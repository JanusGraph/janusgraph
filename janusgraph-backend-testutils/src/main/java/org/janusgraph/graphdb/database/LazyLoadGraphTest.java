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

package org.janusgraph.graphdb.database;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexFilterOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexHasIdOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexHasUniquePropertyOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexIsOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphHasStepStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphIoRegistrationStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphLocalQueryOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMixedIndexAggStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMixedIndexCountStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMultiQueryStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphStepStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphUnusedMultiQueryRemovalStrategy;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;

public class LazyLoadGraphTest extends StandardJanusGraph {

    static {
        TraversalStrategies graphStrategies =
            TraversalStrategies.GlobalCache.getStrategies(Graph.class)
                .clone()
                .addStrategies(AdjacentVertexFilterOptimizerStrategy.instance(),
                    AdjacentVertexHasIdOptimizerStrategy.instance(),
                    AdjacentVertexIsOptimizerStrategy.instance(),
                    AdjacentVertexHasUniquePropertyOptimizerStrategy.instance(),
                    JanusGraphLocalQueryOptimizerStrategy.instance(),
                    JanusGraphHasStepStrategy.instance(),
                    JanusGraphMultiQueryStrategy.instance(),
                    JanusGraphUnusedMultiQueryRemovalStrategy.instance(),
                    JanusGraphMixedIndexAggStrategy.instance(),
                    JanusGraphMixedIndexCountStrategy.instance(),
                    JanusGraphStepStrategy.instance(),
                    JanusGraphIoRegistrationStrategy.instance());

        //Register with cache
        TraversalStrategies.GlobalCache.registerStrategies(LazyLoadGraphTest.class, graphStrategies);
    }

    public LazyLoadGraphTest(GraphDatabaseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public StandardTransactionBuilder buildTransaction() {
        return (StandardTransactionBuilder) new StandardTransactionBuilder(getConfiguration(), this)
            .lazyLoadRelations();
    }
}
