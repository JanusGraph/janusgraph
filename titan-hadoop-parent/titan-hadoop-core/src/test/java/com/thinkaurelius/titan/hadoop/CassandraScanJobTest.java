package com.thinkaurelius.titan.hadoop;


import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.scan.HadoopScanRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class CassandraScanJobTest extends TitanGraphBaseTest {

    private static final Logger log = LoggerFactory.getLogger(CassandraScanJobTest.class);

    @Test
    public void testSimpleHadoopScan()
            throws InterruptedException, ExecutionException, IOException, BackendException {

        int keys = 1000;
        int cols = 40;

        String[][] values = KeyValueStoreUtil.generateData(keys, cols);
        //Make it only half the number of columns for every 2nd key
        for (int i = 0; i < values.length; i++) {
            if (i%2==0) values[i]= Arrays.copyOf(values[i], cols / 2);
        }
        log.debug("Loading values: " + keys + "x" + cols);

        KeyColumnValueStoreManager mgr = new CassandraThriftStoreManager(GraphDatabaseConfiguration.buildGraphConfiguration());
        KeyColumnValueStore store = mgr.openDatabase("edgestore");
        StoreTransaction tx = mgr.beginTransaction(StandardBaseTransactionConfig.of(Timestamps.MICRO));
        KeyColumnValueStoreUtil.loadValues(store, tx, values);
        tx.commit(); // noop on Cassandra, but harmless

        SimpleScanJobRunner runner = (ScanJob job, Configuration jobConf, String rootNSName) -> {
            try {
                return HadoopScanRunner.run(job, jobConf, rootNSName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        };

        SimpleScanJob.runBasicTests(keys, cols, runner);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        String className = getClass().getSimpleName();
        ModifiableConfiguration mc = CassandraStorageSetup.getEmbeddedConfiguration(className);
        return mc.getConfiguration();
    }
}
