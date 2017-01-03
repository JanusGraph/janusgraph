package org.janusgraph.blueprints.process.traversal.strategy;

import org.janusgraph.blueprints.InMemoryGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(JanusStrategySuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = JanusGraph.class)
public class JanusStrategyTest {
}
