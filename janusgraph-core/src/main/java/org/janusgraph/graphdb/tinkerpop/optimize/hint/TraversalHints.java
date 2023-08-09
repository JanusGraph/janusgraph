// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.hint;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;

import java.util.HashMap;
import java.util.Map;

public class TraversalHints {

    public static Configuration from(Traversal<?,?> traversal) {
        Configuration graphConfig = Configuration.EMPTY;
        StandardJanusGraph janusGraph = JanusGraphTraversalUtil.getJanusGraph(traversal.asAdmin());
        if (janusGraph != null) {
            graphConfig = janusGraph.getConfiguration().getConfiguration();
        }

        Map<ConfigElement.PathIdentifier, Object> configMap = new HashMap<>();
        traversal.asAdmin()
            .getStrategies().getStrategy(OptionsStrategy.class)
            .map(OptionsStrategy::getOptions)
            .orElse(new HashMap<>())
            .forEach((key, value) -> {
                configMap.put(ConfigElement.parse(GraphDatabaseConfiguration.ROOT_NS, key), value);
            });

        ModifiableConfiguration hintConfig = GraphDatabaseConfiguration.buildGraphConfiguration();
        hintConfig.setAll(configMap);
        return new MergedConfiguration(hintConfig, graphConfig);
    }
}
