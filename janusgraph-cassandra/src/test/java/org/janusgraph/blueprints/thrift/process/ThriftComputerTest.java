package org.janusgraph.blueprints.thrift.process;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.blueprints.thrift.ThriftGraphComputerProvider;
import org.janusgraph.blueprints.thrift.ThriftGraphProvider;
import org.janusgraph.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = ThriftGraphComputerProvider.class, graph = TitanGraph.class)
public class ThriftComputerTest {

//    TP3 ignores @BeforeClass -- the following method is never executed
//    @BeforeClass
//    public static void beforeSuite() {
//        CassandraStorageSetup.startCleanEmbedded();
//    }

}
