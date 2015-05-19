package com.thinkaurelius.titan.blueprints.groovy;

import com.thinkaurelius.titan.blueprints.InMemoryGraphComputerProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Bryn Cooke
 */
@RunWith(GroovyProcessComputerSuite.class)
@GraphProviderClass(provider = InMemoryGraphComputerProvider.class, graph = TitanGraph.class)
public class InMemoryGroovyProcessComputerTest {
}