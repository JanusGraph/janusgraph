package com.thinkaurelius.titan.hadoop.scan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CassandraHadoopScanRunner {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraHadoopScanRunner.class);

    private static final String CASSANDRA_PARTITIONER_KEY = "cassandra.input.partitioner.class";

    private final ScanJob scanJob;
    private String scanJobConfRoot;
    private Configuration scanJobConf;
    private ReadConfiguration titanConf;
    private org.apache.hadoop.conf.Configuration baseHadoopConf;
    private String partitionerOverride;

    public CassandraHadoopScanRunner(ScanJob scanJob) {
        this.scanJob = scanJob;
    }

    public CassandraHadoopScanRunner scanJobConfRoot(String jobConfRoot) {
        this.scanJobConfRoot = jobConfRoot;
        return this;
    }

    public CassandraHadoopScanRunner scanJobConf(Configuration jobConf) {
        this.scanJobConf = jobConf;
        return this;
    }

    public CassandraHadoopScanRunner baseHadoopConf(org.apache.hadoop.conf.Configuration baseHadoopConf) {
        this.baseHadoopConf = baseHadoopConf;
        return this;
    }

    public CassandraHadoopScanRunner partitionerOverride(String partitionerOverride) {
        this.partitionerOverride = partitionerOverride;
        return this;
    }

    public CassandraHadoopScanRunner useTitanConfiguration(ReadConfiguration titanConf) {
        this.titanConf = titanConf;
        return this;
    }

    public ScanMetrics run() throws InterruptedException, IOException, ClassNotFoundException {

        org.apache.hadoop.conf.Configuration hadoopConf = null != baseHadoopConf ?
                baseHadoopConf : new org.apache.hadoop.conf.Configuration();

        if (null != titanConf) {
            for (String k : titanConf.getKeys("")) {
                hadoopConf.set(ConfigElement.getPath(TitanHadoopConfiguration.TITAN_INPUT_CONFIG_KEYS) + "." + k, titanConf.<Object>get(k, Object.class).toString());
                log.error("Set: {}={}", ConfigElement.getPath(TitanHadoopConfiguration.TITAN_INPUT_CONFIG_KEYS) + "." + k, titanConf.<Object>get(k, Object.class).toString());
            }
        }

        if (null != partitionerOverride) {
            hadoopConf.set(CASSANDRA_PARTITIONER_KEY, partitionerOverride);
        }

        if (null == hadoopConf.get(CASSANDRA_PARTITIONER_KEY)) {
            throw new IllegalArgumentException(CASSANDRA_PARTITIONER_KEY +
                    " must be provided in either the base Hadoop Configuration object or by the partitionerOverride method");
        } else {
            log.debug("Partitioner: {}={}",
                    CASSANDRA_PARTITIONER_KEY, hadoopConf.get(CASSANDRA_PARTITIONER_KEY));
        }

        Preconditions.checkNotNull(hadoopConf);

        return HadoopScanRunner.runJob(scanJob, scanJobConf, scanJobConfRoot, hadoopConf, CassandraBinaryInputFormat.class);
    }
}
