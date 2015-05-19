package com.thinkaurelius.titan.blueprints.structure.groovy;

import com.thinkaurelius.titan.blueprints.BerkeleyGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
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