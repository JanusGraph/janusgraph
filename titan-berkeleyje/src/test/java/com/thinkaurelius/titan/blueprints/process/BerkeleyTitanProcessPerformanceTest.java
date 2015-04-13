package com.thinkaurelius.titan.blueprints.process;

import com.thinkaurelius.titan.blueprints.BerkeleyGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.process.ProcessPerformanceSuite;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Ignore
@RunWith(ProcessPerformanceSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = BerkeleyGraphProvider.class, graph = TitanGraph.class)
public class BerkeleyTitanProcessPerformanceTest {
}
