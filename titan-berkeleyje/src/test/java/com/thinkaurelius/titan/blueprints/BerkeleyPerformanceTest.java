package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Ignore
@RunWith(StructurePerformanceSuite.class)
@StructurePerformanceSuite.GraphProviderClass(provider = BerkeleyGraphProvider.class, graph = TitanGraph.class)
public class BerkeleyPerformanceTest {
}
