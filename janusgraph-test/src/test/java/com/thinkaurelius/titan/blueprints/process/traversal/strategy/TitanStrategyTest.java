package com.thinkaurelius.titan.blueprints.process.traversal.strategy;

import com.thinkaurelius.titan.blueprints.InMemoryGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(TitanStrategySuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = TitanGraph.class)
public class TitanStrategyTest {
}
