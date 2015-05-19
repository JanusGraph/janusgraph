package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;


@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = TitanGraph.class)
public class InMemoryTitanProcessTest {
}