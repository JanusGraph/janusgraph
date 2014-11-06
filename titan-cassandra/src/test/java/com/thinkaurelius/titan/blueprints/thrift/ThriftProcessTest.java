package com.thinkaurelius.titan.blueprints.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = ThriftGraphProvider.class, graph = TitanGraph.class)
public class ThriftProcessTest {

    @BeforeClass
    public static void beforeSuite() {
        CassandraStorageSetup.startCleanEmbedded();
    }

}
