package org.janusgraph.blueprints.process;

import org.janusgraph.blueprints.BerkeleyGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessPerformanceSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(ProcessPerformanceSuite.class)
@GraphProviderClass(provider = BerkeleyGraphProvider.class, graph = JanusGraph.class)
public class BerkeleyJanusProcessPerformanceTest {
}
