package org.janusgraph.blueprints;


import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = MultiQueryJanusGraphProvider.class, graph = JanusGraph.class)
public class MultiQueryJanusGraphProcessTest {
}
