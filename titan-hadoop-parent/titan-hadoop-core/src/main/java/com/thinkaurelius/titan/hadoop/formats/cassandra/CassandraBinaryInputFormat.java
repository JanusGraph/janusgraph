package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.AbstractBinaryInputFormat;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon;
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
 * Wraps a ColumnFamilyInputFormat and converts CFIF's binary types to Titan's binary types.
 */
public class CassandraBinaryInputFormat extends AbstractBinaryInputFormat {

    private static final Logger log = LoggerFactory.getLogger(CassandraBinaryInputFormat.class);

    // Copied these private constants from Cassandra's ConfigHelper circa 2.0.9
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";

    private final ColumnFamilyInputFormat columnFamilyInputFormat = new ColumnFamilyInputFormat();
    private ColumnFamilyRecordReader columnFamilyRecordReader;
    private RecordReader<StaticBuffer, Iterable<Entry>> titanRecordReader;

    public RecordReader<StaticBuffer, Iterable<Entry>> getRecordReader() {
        return titanRecordReader;
    }

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.columnFamilyInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        columnFamilyRecordReader =
                (ColumnFamilyRecordReader)columnFamilyInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        titanRecordReader =
                new CassandraBinaryRecordReader(columnFamilyRecordReader);
        return titanRecordReader;
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);

        // Copy some Titan configuration keys to the Hadoop Configuration keys used by Cassandra's ColumnFamilyInputFormat
        ConfigHelper.setInputInitialAddress(config, titanConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (titanConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            ConfigHelper.setInputRpcPort(config, String.valueOf(titanConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        if (titanConf.has(GraphDatabaseConfiguration.AUTH_USERNAME))
            ConfigHelper.setInputKeyspaceUserName(config, titanConf.get(GraphDatabaseConfiguration.AUTH_USERNAME));
        if (titanConf.has(GraphDatabaseConfiguration.AUTH_PASSWORD))
            ConfigHelper.setInputKeyspacePassword(config, titanConf.get(GraphDatabaseConfiguration.AUTH_PASSWORD));

        // Copy keyspace, force the CF setting to edgestore, honor widerows when set
        final boolean wideRows = config.getBoolean(INPUT_WIDEROWS_CONFIG, false);
        // Use the setInputColumnFamily overload that includes a widerows argument; using the overload without this argument forces it false
        ConfigHelper.setInputColumnFamily(config, titanConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE),
                mrConf.get(TitanHadoopConfiguration.COLUMN_FAMILY_NAME), wideRows);
        log.debug("Set keyspace: {}", titanConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE));

        // Set the column slice bounds via Faunus's vertex query filter
        final SlicePredicate predicate = new SlicePredicate();
        final int rangeBatchSize = config.getInt(RANGE_BATCH_SIZE_CONFIG, Integer.MAX_VALUE);
        predicate.setSlice_range(getSliceRange(TitanHadoopSetupCommon.DEFAULT_SLICE_QUERY, rangeBatchSize)); // TODO stop slicing the whole row
        ConfigHelper.setInputSlicePredicate(config, predicate);
    }

    private SliceRange getSliceRange(final SliceQuery slice, final int limit) {
        final SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(slice.getSliceStart().asByteBuffer());
        sliceRange.setFinish(slice.getSliceEnd().asByteBuffer());
        sliceRange.setCount(Math.min(limit, slice.getLimit()));
        return sliceRange;
    }
}

