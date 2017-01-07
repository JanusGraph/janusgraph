package org.janusgraph.blueprints;

import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;


@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = JanusGraph.class)
public class InMemoryJanusGraphProcessTest {
}
