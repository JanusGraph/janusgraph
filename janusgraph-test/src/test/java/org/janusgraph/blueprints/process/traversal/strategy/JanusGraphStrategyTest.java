package org.janusgraph.blueprints.process.traversal.strategy;

import org.janusgraph.blueprints.InMemoryGraphProvider;
import org.janusgraph.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(TitanStrategySuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = TitanGraph.class)
public class TitanStrategyTest {
}
