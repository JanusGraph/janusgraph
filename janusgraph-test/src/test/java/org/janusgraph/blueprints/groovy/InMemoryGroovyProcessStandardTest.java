package org.janusgraph.blueprints.groovy;

import org.janusgraph.blueprints.InMemoryGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Bryn Cooke
 */
@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = JanusGraph.class)
public class InMemoryGroovyProcessStandardTest {
}
