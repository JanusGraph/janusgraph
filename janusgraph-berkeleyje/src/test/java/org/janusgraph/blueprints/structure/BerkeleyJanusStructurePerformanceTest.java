package org.janusgraph.blueprints.structure;

import org.janusgraph.blueprints.BerkeleyGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Ignore
@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = BerkeleyGraphProvider.class, graph = JanusGraph.class)
public class BerkeleyJanusStructurePerformanceTest {
}
