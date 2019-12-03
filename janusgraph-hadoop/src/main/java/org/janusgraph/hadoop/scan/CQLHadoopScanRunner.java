// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.hadoop.scan;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.olap.VertexScanJob;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.formats.cql.CqlBinaryInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CQLHadoopScanRunner extends AbstractHadoopScanRunner<CQLHadoopScanRunner> {

    private static final Logger log =
            LoggerFactory.getLogger(CQLHadoopScanRunner.class);

    private static final String CASSANDRA_PARTITIONER_KEY = "cassandra.input.partitioner.class";

    private String partitionerOverride;

    public CQLHadoopScanRunner(ScanJob scanJob) {
        super(scanJob);
    }

    public CQLHadoopScanRunner(VertexScanJob vertexScanJob) {
        super(vertexScanJob);
    }

    protected CQLHadoopScanRunner self() {
        return this;
    }

    public CQLHadoopScanRunner partitionerOverride(String partitionerOverride) {
        this.partitionerOverride = partitionerOverride;
        return this;
    }

    public ScanMetrics run() throws InterruptedException, IOException, ClassNotFoundException {

        org.apache.hadoop.conf.Configuration hadoopConf = null != baseHadoopConf ?
                baseHadoopConf : new org.apache.hadoop.conf.Configuration();

        if (null != janusgraphConf) {
            for (String k : janusgraphConf.getKeys("")) {
                String prefix = ConfigElement.getPath(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
                hadoopConf.set(prefix + k, janusgraphConf.get(k, Object.class).toString());
                log.debug("Set: {}={}", prefix + k,
                        janusgraphConf.get(k, Object.class).toString());
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
            return HadoopScanRunner.runScanJob(scanJob, scanJobConf, scanJobConfRoot, hadoopConf, CqlBinaryInputFormat.class);
        } else {
            return HadoopScanRunner.runVertexScanJob(vertexScanJob, scanJobConf, scanJobConfRoot, hadoopConf, CqlBinaryInputFormat.class);
        }
    }
}
