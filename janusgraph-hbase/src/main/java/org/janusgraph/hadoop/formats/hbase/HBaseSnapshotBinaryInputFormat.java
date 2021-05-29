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

package org.janusgraph.hadoop.formats.hbase;

import com.google.common.collect.BiMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.formats.util.AbstractBinaryInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * An input format to read from a HBase snapshot. This will consume a stable,
 * read-only view of a HBase table directly off HDFS, bypassing HBase server
 * calls. The configuration properties required by this input format are:
 * <p>
 * 1. The snapshot name. This points to a pre-created snapshot of the graph
 * table on HBase. {@link HBaseStoreManager#HBASE_SNAPSHOT}
 * <br>
 * e.g. janusgraphmr.ioformat.conf.storage.hbase.snapshot-name=janusgraph-snapshot
 * <p>
 * 2. The snapshot restore directory. This is specified as a temporary restore
 * directory on the same File System as hbase root dir. The restore directory is
 * used to restore the table and region structure from the snapshot to scan the
 * table, but with no data coping involved.
 * {@link HBaseStoreManager#HBASE_SNAPSHOT_RESTORE_DIR}
 * <br>
 * e.g. janusgraphmr.ioformat.conf.storage.hbase.snapshot-restore-dir=/tmp
 * <p>
 * It is also required that the Hadoop configuration directory, which contains
 * core-site.xml, is in the classpath for access to the hadoop cluster. This
 * requirement is similar to the configuration requirement for <a href=
 * "https://tinkerpop.apache.org/docs/current/reference/#hadoop-gremlin">hadoop-gremlin</a>
 * <p>
 * Additionally, the HBase configuration directory, which contains hbase-site.xml,
 * should be placed in the classpath as well. If it is not, hbase.rootdir property
 * needs to be set as a pass-through property in the graph property file.
 * <br>
 * e.g. janusgraphmr.ioformat.conf.storage.hbase.ext.hbase.rootdir=/hbase
 */
public class HBaseSnapshotBinaryInputFormat extends AbstractBinaryInputFormat {

    private static final Logger log = LoggerFactory.getLogger(HBaseSnapshotBinaryInputFormat.class);

    // Key for specifying the snapshot name. To be replaced by the constant in hbase package if it
    // becomes public.
    private static final String SNAPSHOT_NAME_KEY = "hbase.TableSnapshotInputFormat.snapshot.name";
    // key for specifying the root dir of the restored snapshot. To be replaced by the constant
    // in hbase package if it becomes public.
    private static final String RESTORE_DIR_KEY = "hbase.TableSnapshotInputFormat.restore.dir";

    private final TableSnapshotInputFormat tableSnapshotInputFormat = new TableSnapshotInputFormat();
    private RecordReader<ImmutableBytesWritable, Result> tableReader;
    private byte[] edgeStoreFamily;
    private RecordReader<StaticBuffer, Iterable<Entry>> janusgraphRecordReader;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableSnapshotInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        tableReader = tableSnapshotInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        janusgraphRecordReader = new HBaseBinaryRecordReader(tableReader, edgeStoreFamily);
        return janusgraphRecordReader;
    }

    @Override
    public void setConf(final Configuration config) {
        HBaseConfiguration.addHbaseResources(config);
        super.setConf(config);

        // Pass the extra pass-through properties directly to HBase/Hadoop config.
        final Map<String, Object> configSub = janusgraphConf.getSubset(HBaseStoreManager.HBASE_CONFIGURATION_NAMESPACE);
        for (Map.Entry<String, Object> entry : configSub.entrySet()) {
            log.info("HBase configuration: setting {}={}", entry.getKey(), entry.getValue());
            if (entry.getValue() == null) continue;
            config.set(entry.getKey(), entry.getValue().toString());
        }

        config.set("autotype", "none");
        final Scan scanner = new Scan();
        String cfName = mrConf.get(JanusGraphHadoopConfiguration.COLUMN_FAMILY_NAME);
        // TODO the space-saving short name mapping leaks from HBaseStoreManager here
        if (janusgraphConf.get(HBaseStoreManager.SHORT_CF_NAMES)) {
            try {
                final BiMap<String, String> shortCfMap = HBaseStoreManager.createShortCfMap(janusgraphConf);
                cfName = HBaseStoreManager.shortenCfName(shortCfMap, cfName);
            } catch (PermanentBackendException e) {
                throw new RuntimeException(e);
            }
        }
        edgeStoreFamily = Bytes.toBytes(cfName);
        scanner.addFamily(edgeStoreFamily);

        // This is a workaround, to be removed when convertScanToString becomes public in hbase package.
        Method converter;
        try {
            converter = TableMapReduceUtil.class.getDeclaredMethod("convertScanToString", Scan.class);
            converter.setAccessible(true);
            config.set(TableInputFormat.SCAN, (String) converter.invoke(null, scanner));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        final String snapshotName = janusgraphConf.get(HBaseStoreManager.HBASE_SNAPSHOT);
        final String restoreDirString = janusgraphConf.get(HBaseStoreManager.HBASE_SNAPSHOT_RESTORE_DIR);

        final Path restoreDir = new Path(restoreDirString);
        try {
            // This is a workaround. TableSnapshotInputFormat.setInput accepts a Job as parameter.
            // And the Job.getInstance(config) create clone of the config, not setting on the
            // passed in config.
            Job job = Job.getInstance(config);
            TableSnapshotInputFormat.setInput(job, snapshotName, restoreDir);
            config.set(SNAPSHOT_NAME_KEY, job.getConfiguration().get(SNAPSHOT_NAME_KEY));
            config.set(RESTORE_DIR_KEY, job.getConfiguration().get(RESTORE_DIR_KEY));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf() {
        return super.getConf();
    }
}
