package com.thinkaurelius.titan.hadoop;


import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProviders;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraInputFormat;
import com.thinkaurelius.titan.hadoop.scan.CassandraHadoopScanRunner;
import com.thinkaurelius.titan.hadoop.scan.HadoopScanMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class CassandraScanJobIT extends TitanGraphBaseTest {

    private static final Logger log = LoggerFactory.getLogger(CassandraScanJobIT.class);

    @Test
    public void testSimpleScan()
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
        StoreTransaction tx = mgr.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        KeyColumnValueStoreUtil.loadValues(store, tx, values);
        tx.commit(); // noop on Cassandra, but harmless

        SimpleScanJobRunner runner = (ScanJob job, Configuration jobConf, String rootNSName) -> {
            try {
                return new CassandraHadoopScanRunner(job).scanJobConf(jobConf).scanJobConfRoot(rootNSName)
                        .partitionerOverride("org.apache.cassandra.dht.Murmur3Partitioner").run();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        };

        SimpleScanJob.runBasicTests(keys, cols, runner);
    }

    @Test
    public void testPartitionedVertexScan() throws Exception {
        tearDown();
        clearGraph(getConfiguration());
        WriteConfiguration partConf = getConfiguration();
        open(partConf);
        mgmt.makeVertexLabel("part").partition().make();
        finishSchema();
        TitanVertex supernode = graph.addVertex("part");
        for (int i = 0; i < 128; i++) {
            TitanVertex v = graph.addVertex("part");
            v.addEdge("default", supernode);
            if (0 < i && 0 == i % 4)
                graph.tx().commit();
        }
        graph.tx().commit();

        org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration();
        c.set(ConfigElement.getPath(TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + "." + "storage.cassandra.keyspace", getClass().getSimpleName());
        c.set(ConfigElement.getPath(TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + "." + "storage.backend", "cassandrathrift");
        c.set("cassandra.input.partitioner.class", "org.apache.cassandra.dht.Murmur3Partitioner");

        Job job = getVertexJobWithDefaultMapper(c);

        // Should throw an exception since filter-partitioned-vertices wasn't enabled
        assertFalse(job.waitForCompletion(true));
    }

    @Test
    public void testPartitionedVertexFilteredScan() throws Exception {
        tearDown();
        clearGraph(getConfiguration());
        WriteConfiguration partConf = getConfiguration();
        open(partConf);
        mgmt.makeVertexLabel("part").partition().make();
        finishSchema();
        TitanVertex supernode = graph.addVertex("part");
        for (int i = 0; i < 128; i++) {
            TitanVertex v = graph.addVertex("part");
            v.addEdge("default", supernode);
            if (0 < i && 0 == i % 4)
                graph.tx().commit();
        }
        graph.tx().commit();

        org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration();
        c.set(ConfigElement.getPath(TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + "." + "storage.cassandra.keyspace", getClass().getSimpleName());
        c.set(ConfigElement.getPath(TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + "." + "storage.backend", "cassandrathrift");
        c.set(ConfigElement.getPath(TitanHadoopConfiguration.FILTER_PARTITIONED_VERTICES), "true");
        c.set("cassandra.input.partitioner.class", "org.apache.cassandra.dht.Murmur3Partitioner");

        Job job = getVertexJobWithDefaultMapper(c);

        // Should succeed
        assertTrue(job.waitForCompletion(true));
    }

    private Job getVertexJobWithDefaultMapper(org.apache.hadoop.conf.Configuration c) throws IOException {

        Job job = Job.getInstance(c);

        job.setJarByClass(HadoopScanMapper.class);
        job.setJobName("testPartitionedVertexScan");
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setInputFormatClass(CassandraInputFormat.class);

        return job;
    }

    @Override
    public WriteConfiguration getConfiguration() {
        String className = getClass().getSimpleName();
        ModifiableConfiguration mc = CassandraStorageSetup.getEmbeddedConfiguration(className);
        return mc.getConfiguration();
    }

//    public static class NoopScanJob implements ScanJob {
//
//        @Override
//        public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
//            // do nothing
//        }
//
//        @Override
//        public List<SliceQuery> getQueries() {
//            int len = 4;
//            return ImmutableList.of(new SliceQuery(BufferUtil.zeroBuffer(len), BufferUtil.oneBuffer(len)));
//        }
//    }
}
