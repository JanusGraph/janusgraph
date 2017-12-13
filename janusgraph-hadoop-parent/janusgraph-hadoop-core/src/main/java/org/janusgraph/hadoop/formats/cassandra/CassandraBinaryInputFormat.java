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

package org.janusgraph.hadoop.formats.cassandra;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.formats.util.AbstractBinaryInputFormat;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetupCommon;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Wraps a ColumnFamilyInputFormat and converts CFIF's binary types to JanusGraph's binary types.
 */
public class CassandraBinaryInputFormat extends AbstractBinaryInputFormat {

    private static final Logger log = LoggerFactory.getLogger(CassandraBinaryInputFormat.class);

    // Copied these private constants from Cassandra's ConfigHelper circa 2.0.9
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";

    private final ColumnFamilyInputFormat columnFamilyInputFormat = new ColumnFamilyInputFormat();
    RecordReader<StaticBuffer, Iterable<Entry>> janusgraphRecordReader;

    public RecordReader<StaticBuffer, Iterable<Entry>> getRecordReader() {
        return janusgraphRecordReader;
    }

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.columnFamilyInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        janusgraphRecordReader = new CassandraBinaryRecordReader(
                (ColumnFamilyRecordReader) columnFamilyInputFormat.createRecordReader(inputSplit, taskAttemptContext));
        return janusgraphRecordReader;
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);

        // Copy some JanusGraph configuration keys to the Hadoop Configuration keys used by Cassandra's ColumnFamilyInputFormat
        ConfigHelper.setInputInitialAddress(config, janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (janusgraphConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            ConfigHelper.setInputRpcPort(config, String.valueOf(janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        if (janusgraphConf.has(GraphDatabaseConfiguration.AUTH_USERNAME))
            ConfigHelper.setInputKeyspaceUserName(config, janusgraphConf.get(GraphDatabaseConfiguration.AUTH_USERNAME));
        if (janusgraphConf.has(GraphDatabaseConfiguration.AUTH_PASSWORD))
            ConfigHelper.setInputKeyspacePassword(config, janusgraphConf.get(GraphDatabaseConfiguration.AUTH_PASSWORD));

        // Copy keyspace, force the CF setting to edgestore, honor widerows when set
        final boolean wideRows = config.getBoolean(INPUT_WIDEROWS_CONFIG, false);
        // Use the setInputColumnFamily overload that includes a widerows argument; using the overload without this argument forces it false
        ConfigHelper.setInputColumnFamily(config, janusgraphConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE),
                mrConf.get(JanusGraphHadoopConfiguration.COLUMN_FAMILY_NAME), wideRows);
        log.debug("Set keyspace: {}", janusgraphConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE));

        // Set the column slice bounds via Faunus' vertex query filter
        final SlicePredicate predicate = new SlicePredicate();
        final int rangeBatchSize = config.getInt(RANGE_BATCH_SIZE_CONFIG, Integer.MAX_VALUE);
        predicate.setSlice_range(getSliceRange(rangeBatchSize)); // TODO stop slicing the whole row
        ConfigHelper.setInputSlicePredicate(config, predicate);
    }

    private SliceRange getSliceRange(final int limit) {
        final SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getSliceStart().asByteBuffer());
        sliceRange.setFinish(JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getSliceEnd().asByteBuffer());
        sliceRange.setCount(Math.min(limit, JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getLimit()));
        return sliceRange;
    }
}

