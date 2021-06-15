// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.blueprints;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMultiQueryStrategy;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.HashMap;
import java.util.Map;

/**
 * This class allows to exclude certain strategies for specified tests.
 * It can sometimes be necessary to do so, in case one of the TinkerPop
 * tests expects a specific number of steps in a query. (e.g. ProfileTest)
 */
public class JanusGraphTestListener implements GraphProvider.TestListener {

    private static final JanusGraphTestListener INSTANCE = new JanusGraphTestListener();

    private static final Map<Class<?>, TraversalStrategy[]> testSpecificDisabledStrategies;

    static {
        testSpecificDisabledStrategies = new HashMap<>();

        // reference: https://groups.google.com/g/gremlin-users/c/93dssDZVJEM
        testSpecificDisabledStrategies.put(ProfileTest.Traversals.class, new TraversalStrategy[] {JanusGraphMultiQueryStrategy.instance()});
    }

    private JanusGraphTestListener() {
    }

    public static JanusGraphTestListener instance() {
        return INSTANCE;
    }

    public void onTestStart(final Class<?> test, final String testName) {
        if (testSpecificDisabledStrategies.containsKey(test)) {
            /*
             * disable test-specific strategies
             */
            TraversalStrategy[] disabledStrategies = testSpecificDisabledStrategies.get(test);
            Class[] strategyClasses = new Class[disabledStrategies.length];
            for (int i = 0; i < disabledStrategies.length; i++) {
                strategyClasses[i] = disabledStrategies[i].getClass();
            }
            TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(StandardJanusGraph.class).clone()
                .removeStrategies(strategyClasses));
            TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraphTx.class,
                TraversalStrategies.GlobalCache.getStrategies(StandardJanusGraphTx.class).clone()
                .removeStrategies(strategyClasses));
        }
    }

    public void onTestEnd(final Class<?> test, final String testName) {
        if (testSpecificDisabledStrategies.containsKey(test)) {
            /*
             * re-enable test-specific strategies
             */
            TraversalStrategy[] disabledStrategies = testSpecificDisabledStrategies.get(test);
            TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(StandardJanusGraph.class).clone()
                .addStrategies(disabledStrategies));
            TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraphTx.class,
                TraversalStrategies.GlobalCache.getStrategies(StandardJanusGraphTx.class).clone()
                .addStrategies(disabledStrategies));
        }
    }
}
