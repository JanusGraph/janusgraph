package org.janusgraph.blueprints;

import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;


@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = JanusGraph.class)
public class InMemoryJanusStructureTest {
}
