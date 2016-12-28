package org.janusgraph.blueprints.process;

import org.janusgraph.blueprints.BerkeleyGraphComputerProvider;
import org.janusgraph.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = BerkeleyGraphComputerProvider.class, graph = TitanGraph.class)
public class BerkeleyTitanComputerTest {
}
