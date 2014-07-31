package com.thinkaurelius.titan.hadoop.formats.titan.cassandra;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.TitanOutputFormatTest;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraOutputFormatTest extends TitanOutputFormatTest {

    @BeforeClass
    public static void startUpCassandra() throws Exception {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    protected void setCustomFaunusOptions(TitanHadoopConfiguration.ModifiableHadoopConfiguration c) {
        c.getHadoopConfiguration().set(
                "cassandra.input.partitioner.class",
                "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    @Override
    protected ModifiableConfiguration getTitanConfiguration() {
        String className = TitanCassandraOutputFormatTest.class.getSimpleName();
        ModifiableConfiguration mc = CassandraStorageSetup.getCassandraThriftConfiguration(className);
        mc.set(STORAGE_HOSTS, new String[]{"localhost"});
        mc.set(DB_CACHE, false);
        mc.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
        return mc;
    }

    @Override
    protected Class<?> getTitanInputFormatClass() {
        return TitanCassandraInputFormat.class;
    }

    @Override
    protected Class<?> getTitanOutputFormatClass() {
        return TitanCassandraOutputFormat.class;
    }
}
