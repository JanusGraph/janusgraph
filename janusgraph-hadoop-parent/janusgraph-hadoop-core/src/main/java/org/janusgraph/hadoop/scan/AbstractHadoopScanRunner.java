package org.janusgraph.hadoop.scan;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.graphdb.olap.VertexScanJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractHadoopScanRunner<R> {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraHadoopScanRunner.class);

    protected final ScanJob scanJob;
    protected final VertexScanJob vertexScanJob;
    protected String scanJobConfRoot;
    protected Configuration scanJobConf;
    protected ReadConfiguration titanConf;
    protected org.apache.hadoop.conf.Configuration baseHadoopConf;

    public AbstractHadoopScanRunner(ScanJob scanJob) {
        this.scanJob = scanJob;
        this.vertexScanJob = null;
    }

    public AbstractHadoopScanRunner(VertexScanJob vertexScanJob) {
        this.vertexScanJob = vertexScanJob;
        this.scanJob = null;
    }

    protected abstract R self();

    public R scanJobConfRoot(String jobConfRoot) {
        this.scanJobConfRoot = jobConfRoot;
        return self();
    }

    public R scanJobConf(Configuration jobConf) {
        this.scanJobConf = jobConf;
        return self();
    }

    public R baseHadoopConf(org.apache.hadoop.conf.Configuration baseHadoopConf) {
        this.baseHadoopConf = baseHadoopConf;
        return self();
    }

    public R useTitanConfiguration(ReadConfiguration titanConf) {
        this.titanConf = titanConf;
        return self();
    }
}
