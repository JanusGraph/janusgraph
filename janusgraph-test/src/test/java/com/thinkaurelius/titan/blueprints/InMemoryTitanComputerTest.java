package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = InMemoryGraphComputerProvider.class, graph = TitanGraph.class)
public class InMemoryTitanComputerTest {
}