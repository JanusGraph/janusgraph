package org.janusgraph.blueprints;

import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = InMemoryGraphComputerProvider.class, graph = JanusGraph.class)
public class InMemoryJanusComputerTest {
}
