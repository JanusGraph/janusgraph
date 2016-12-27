package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;


@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = TitanGraph.class)
public class InMemoryTitanStructureTest {
}
