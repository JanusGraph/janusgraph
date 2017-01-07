package org.janusgraph.blueprints.structure.groovy;

import org.janusgraph.blueprints.BerkeleyGraphProvider;
import org.janusgraph.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentPerformanceSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Bryn Cooke
 */
@Ignore
@RunWith(GroovyEnvironmentPerformanceSuite.class)
@GraphProviderClass(provider = BerkeleyGraphProvider.class, graph = TitanGraph.class)
public class BerkeleyTitanGroovyEnvironmentPerformanceTest {
}
