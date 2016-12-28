package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.olap.job.IndexRemoveJob;
import com.thinkaurelius.titan.graphdb.olap.job.IndexRepairJob;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.scan.CassandraHadoopScanRunner;
import com.thinkaurelius.titan.hadoop.scan.HBaseHadoopScanRunner;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class MapReduceIndexJobs {

    private static final Logger log =
            LoggerFactory.getLogger(MapReduceIndexJobs.class);

    public static ScanMetrics cassandraRepair(String titanPropertiesPath, String indexName, String relationType, String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(titanPropertiesPath);
            p.load(fis);
            return cassandraRepair(p, indexName, relationType, partitionerName);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics cassandraRepair(Properties titanProperties, String indexName, String relationType,
                                              String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        return cassandraRepair(titanProperties, indexName, relationType, partitionerName, new Configuration());
    }

    public static ScanMetrics cassandraRepair(Properties titanProperties, String indexName, String relationType,
                                              String partitionerName, Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRepairJob job = new IndexRepairJob();
        CassandraHadoopScanRunner cr = new CassandraHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, titanProperties);
        cr.partitionerOverride(partitionerName);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }


    public static ScanMetrics cassandraRemove(String titanPropertiesPath, String indexName, String relationType, String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(titanPropertiesPath);
            p.load(fis);
            return cassandraRemove(p, indexName, relationType, partitionerName);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics cassandraRemove(Properties titanProperties, String indexName, String relationType,
                                              String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        return cassandraRemove(titanProperties, indexName, relationType, partitionerName, new Configuration());
    }

    public static ScanMetrics cassandraRemove(Properties titanProperties, String indexName, String relationType,
                                              String partitionerName, Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRemoveJob job = new IndexRemoveJob();
        CassandraHadoopScanRunner cr = new CassandraHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, titanProperties);
        cr.partitionerOverride(partitionerName);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }

    public static ScanMetrics hbaseRepair(String titanPropertiesPath, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(titanPropertiesPath);
            p.load(fis);
            return hbaseRepair(p, indexName, relationType);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics hbaseRepair(Properties titanProperties, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        return hbaseRepair(titanProperties, indexName, relationType, new Configuration());
    }

    public static ScanMetrics hbaseRepair(Properties titanProperties, String indexName, String relationType,
                                          Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRepairJob job = new IndexRepairJob();
        HBaseHadoopScanRunner cr = new HBaseHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, titanProperties);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }

    public static ScanMetrics hbaseRemove(String titanPropertiesPath, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(titanPropertiesPath);
            p.load(fis);
            return hbaseRemove(p, indexName, relationType);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics hbaseRemove(Properties titanProperties, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        return hbaseRemove(titanProperties, indexName, relationType, new Configuration());
    }

    public static ScanMetrics hbaseRemove(Properties titanProperties, String indexName, String relationType,
                                          Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRemoveJob job = new IndexRemoveJob();
        HBaseHadoopScanRunner cr = new HBaseHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, titanProperties);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }

    private static ModifiableConfiguration getIndexJobConf(String indexName, String relationType) {
        ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.JOB_NS,
                new CommonsConfiguration(new BaseConfiguration()), BasicConfiguration.Restriction.NONE);
        mc.set(com.thinkaurelius.titan.graphdb.olap.job.IndexUpdateJob.INDEX_NAME, indexName);
        mc.set(com.thinkaurelius.titan.graphdb.olap.job.IndexUpdateJob.INDEX_RELATION_TYPE, relationType);
        mc.set(GraphDatabaseConfiguration.JOB_START_TIME, System.currentTimeMillis());
        return mc;
    }

    private static void copyPropertiesToInputAndOutputConf(Configuration sink, Properties source) {
        final String prefix = ConfigElement.getPath(TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
        for (Map.Entry<Object, Object> e : source.entrySet()) {
            String k;
            String v = e.getValue().toString();
            k = prefix + e.getKey().toString();
            sink.set(k, v);
            log.info("Set {}={}", k, v);
        }
    }
}
