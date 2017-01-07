package org.janusgraph.blueprints.process.groovy;

import org.janusgraph.blueprints.BerkeleyGraphComputerProvider;
import org.janusgraph.blueprints.BerkeleyGraphProvider;
import org.janusgraph.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Bryn Cooke
 */
@RunWith(GroovyProcessComputerSuite.class)
@GraphProviderClass(provider = BerkeleyGraphComputerProvider.class, graph = TitanGraph.class)
public class BerkeleyTitanGroovyProcessComputerTest {
}
