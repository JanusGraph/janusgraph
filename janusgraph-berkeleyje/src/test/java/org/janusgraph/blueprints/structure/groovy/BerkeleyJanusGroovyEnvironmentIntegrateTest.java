package org.janusgraph.blueprints.structure.groovy;

import org.janusgraph.blueprints.BerkeleyGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Bryn Cooke
 */
@Ignore
@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = BerkeleyGraphProvider.class, graph = JanusGraph.class)
public class BerkeleyJanusGroovyEnvironmentIntegrateTest {
}
