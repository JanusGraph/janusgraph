package org.janusgraph.blueprints;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = JanusGraphProvider.class, graph = JanusGraph.class)
public class JanusGraphStructureTest {
}
