package com.thinkaurelius.titan.blueprints.groovy;

import com.thinkaurelius.titan.blueprints.InMemoryGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Bryn Cooke
 */
@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = TitanGraph.class)
public class InMemoryGroovyProcessStandardTest {
}