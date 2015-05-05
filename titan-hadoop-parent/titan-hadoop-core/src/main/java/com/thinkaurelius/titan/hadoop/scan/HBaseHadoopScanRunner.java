package com.thinkaurelius.titan.hadoop.scan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.hbase.HBaseBinaryInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// TODO port HBase-Kerberos auth token support
public class HBaseHadoopScanRunner extends AbstractHadoopScanRunner<HBaseHadoopScanRunner> {

    private static final Logger log = LoggerFactory.getLogger(HBaseHadoopScanRunner.class);

    public HBaseHadoopScanRunner(ScanJob scanJob) {
        super(scanJob);
    }

    public HBaseHadoopScanRunner(VertexScanJob vertexScanJob) {
        super(vertexScanJob);
    }

    @Override
    protected HBaseHadoopScanRunner self() {
        return this;
    }

    public ScanMetrics run() throws InterruptedException, IOException, ClassNotFoundException {

        org.apache.hadoop.conf.Configuration hadoopConf = null != baseHadoopConf ?
                baseHadoopConf : new org.apache.hadoop.conf.Configuration();

        if (null != titanConf) {
            String prefix = ConfigElement.getPath(TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
            for (String k : titanConf.getKeys("")) {
                hadoopConf.set(prefix + k, titanConf.get(k, Object.class).toString());
                log.debug("Set: {}={}", prefix + k, titanConf.get(k, Object.class).toString());
            }
        }
        Preconditions.checkNotNull(hadoopConf);

        if (null != scanJob) {
            return HadoopScanRunner.runScanJob(scanJob, scanJobConf, scanJobConfRoot, hadoopConf, HBaseBinaryInputFormat.class);
        } else {
            return HadoopScanRunner.runVertexScanJob(vertexScanJob, scanJobConf, scanJobConfRoot, hadoopConf, HBaseBinaryInputFormat.class);
        }
    }
}
