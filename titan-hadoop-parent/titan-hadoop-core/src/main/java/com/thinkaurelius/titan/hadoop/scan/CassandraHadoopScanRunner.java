package com.thinkaurelius.titan.hadoop.scan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CassandraHadoopScanRunner extends AbstractHadoopScanRunner<CassandraHadoopScanRunner> {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraHadoopScanRunner.class);

    private static final String CASSANDRA_PARTITIONER_KEY = "cassandra.input.partitioner.class";

    private String partitionerOverride;

    public CassandraHadoopScanRunner(ScanJob scanJob) {
        super(scanJob);
    }

    public CassandraHadoopScanRunner(VertexScanJob vertexScanJob) {
        super(vertexScanJob);
    }

    protected CassandraHadoopScanRunner self() {
        return this;
    }

    public CassandraHadoopScanRunner partitionerOverride(String partitionerOverride) {
        this.partitionerOverride = partitionerOverride;
        return this;
    }

    public ScanMetrics run() throws InterruptedException, IOException, ClassNotFoundException {

        org.apache.hadoop.conf.Configuration hadoopConf = null != baseHadoopConf ?
                baseHadoopConf : new org.apache.hadoop.conf.Configuration();

        if (null != titanConf) {
            for (String k : titanConf.getKeys("")) {
                String prefix = ConfigElement.getPath(TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
                hadoopConf.set(prefix + k, titanConf.get(k, Object.class).toString());
                log.debug("Set: {}={}", prefix + k,
                        titanConf.<Object>get(k, Object.class).toString());
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

        if (null != scanJob) {
            return HadoopScanRunner.runScanJob(scanJob, scanJobConf, scanJobConfRoot, hadoopConf, CassandraBinaryInputFormat.class);
        } else {
            return HadoopScanRunner.runVertexScanJob(vertexScanJob, scanJobConf, scanJobConfRoot, hadoopConf, CassandraBinaryInputFormat.class);
        }
    }
}
