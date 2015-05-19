package com.thinkaurelius.titan.blueprints.thrift.structure;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.blueprints.thrift.ThriftGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = ThriftGraphProvider.class, graph = TitanGraph.class)
public class ThriftStructureTest {

//    TP3 ignores @BeforeClass -- the following method is never executed
//    @BeforeClass
//    public static void beforeSuite() {
//        CassandraStorageSetup.startCleanEmbedded();
//    }

}
