package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.formats.TitanOutputFormatTest;

import org.junit.BeforeClass;

import java.io.IOException;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
* @author Marko A. Rodriguez (http://markorodriguez.com)
*/
public class TitanHBaseOutputFormatTest extends TitanOutputFormatTest {

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @Override
    protected ModifiableConfiguration getTitanConfiguration() {
        ModifiableConfiguration config = new ModifiableConfiguration(ROOT_NS,
                new CommonsConfiguration(),
                BasicConfiguration.Restriction.NONE);

        config.set(STORAGE_BACKEND, "hbase");
        config.set(STORAGE_HOSTS, new String[]{"localhost"});
        config.set(DB_CACHE, false);
        config.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");

        return config;
    }

    @Override
    protected Class<?> getTitanInputFormatClass() {
        return TitanHBaseInputFormat.class;
    }

    @Override
    protected Class<?> getTitanOutputFormatClass() {
        return TitanHBaseOutputFormat.class;
    }
}
