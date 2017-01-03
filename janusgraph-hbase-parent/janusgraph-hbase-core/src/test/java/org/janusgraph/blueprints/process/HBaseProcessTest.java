package org.janusgraph.blueprints.process;

import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.blueprints.HBaseGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = HBaseGraphProvider.class, graph = JanusGraph.class)
public class HBaseProcessTest {

    @BeforeClass
    public static void startHBase() {
        try {
            HBaseStorageSetup.startHBase();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }
}
