package org.janusgraph.blueprints;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = JanusGraphComputerProvider.class, graph = JanusGraph.class)
public class JanusGraphComputerTest {
}
