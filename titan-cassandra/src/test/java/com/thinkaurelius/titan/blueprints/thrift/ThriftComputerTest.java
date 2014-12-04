package com.thinkaurelius.titan.blueprints.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(ProcessComputerSuite.class)
@ProcessComputerSuite.GraphProviderClass(provider = ThriftGraphProvider.class, graph = TitanGraph.class)
public class ThriftComputerTest {

    @BeforeClass
    public static void beforeSuite() {
        CassandraStorageSetup.startCleanEmbedded();
    }

}