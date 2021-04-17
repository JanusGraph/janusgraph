// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.olap.job.IndexRemoveJob;
import org.janusgraph.graphdb.olap.job.IndexRepairJob;
import org.janusgraph.hadoop.scan.HBaseHadoopScanRunner;
import org.janusgraph.util.system.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class HBaseMapReduceIndexJobsUtils {

    public static ScanMetrics repair(String janusgraphPropertiesPath, String indexName, String relationType)
        throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(janusgraphPropertiesPath);
            p.load(fis);
            return repair(p, indexName, relationType);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics repair(Properties janusgraphProperties, String indexName, String relationType)
        throws InterruptedException, IOException, ClassNotFoundException {
        return repair(janusgraphProperties, indexName, relationType, new Configuration());
    }

    public static ScanMetrics repair(Properties janusgraphProperties, String indexName, String relationType,
                                     Configuration hadoopBaseConf)
        throws InterruptedException, IOException, ClassNotFoundException {
        IndexRepairJob job = new IndexRepairJob();
        HBaseHadoopScanRunner cr = new HBaseHadoopScanRunner(job);
        return executeScanRunner(janusgraphProperties, indexName, relationType, hadoopBaseConf, cr);
    }

    public static ScanMetrics remove(String janusgraphPropertiesPath, String indexName, String relationType)
        throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(janusgraphPropertiesPath);
            p.load(fis);
            return remove(p, indexName, relationType);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics remove(Properties janusgraphProperties, String indexName, String relationType)
        throws InterruptedException, IOException, ClassNotFoundException {
        return remove(janusgraphProperties, indexName, relationType, new Configuration());
    }

    public static ScanMetrics remove(Properties janusgraphProperties, String indexName, String relationType,
                                     Configuration hadoopBaseConf)
        throws InterruptedException, IOException, ClassNotFoundException {
        IndexRemoveJob job = new IndexRemoveJob();
        HBaseHadoopScanRunner cr = new HBaseHadoopScanRunner(job);
        return executeScanRunner(janusgraphProperties, indexName, relationType, hadoopBaseConf, cr);
    }

    private static ScanMetrics executeScanRunner(Properties janusgraphProperties, String indexName, String relationType, Configuration hadoopBaseConf, HBaseHadoopScanRunner cr) throws InterruptedException, IOException, ClassNotFoundException {
        ModifiableConfiguration mc = MapReduceIndexJobs.getIndexJobConf(indexName, relationType);
        MapReduceIndexJobs.copyPropertiesToInputAndOutputConf(hadoopBaseConf, janusgraphProperties);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }
}
