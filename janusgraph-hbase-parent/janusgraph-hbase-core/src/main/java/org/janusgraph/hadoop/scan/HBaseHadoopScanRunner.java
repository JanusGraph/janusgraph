// Copyright 2017 JanusGraph Authors
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
import org.janusgraph.hadoop.formats.hbase.HBaseBinaryInputFormat;
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

        if (null != janusgraphConf) {
            String prefix = ConfigElement.getPath(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
            for (String k : janusgraphConf.getKeys("")) {
                hadoopConf.set(prefix + k, janusgraphConf.get(k, Object.class).toString());
                log.debug("Set: {}={}", prefix + k, janusgraphConf.get(k, Object.class).toString());
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
