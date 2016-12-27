package com.thinkaurelius.titan.blueprints.structure;

import com.thinkaurelius.titan.blueprints.BerkeleyGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = BerkeleyGraphProvider.class, graph = TitanGraph.class)
public class BerkeleyTitanStructureTest {
}
