package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Wraps a ColumnFamilyRecordReader and converts CFRR's binary types to Titan binary types.
 */
public class CassandraBinaryRecordReader extends RecordReader<StaticBuffer, Iterable<Entry>> {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraBinaryRecordReader.class);

    private ColumnFamilyRecordReader reader;

    public CassandraBinaryRecordReader(final ColumnFamilyRecordReader reader) {
        this.reader = reader;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public StaticBuffer getCurrentKey() throws IOException, InterruptedException {
        return StaticArrayBuffer.of(reader.getCurrentKey());
    }

    @Override
    public Iterable<Entry> getCurrentValue() throws IOException, InterruptedException {
        return new CassandraMapIterable(reader.getCurrentValue());
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public float getProgress() {
        return reader.getProgress();
    }

    private static class CassandraMapIterable implements Iterable<Entry> {

        private final SortedMap<ByteBuffer, Column> columnValues;

        public CassandraMapIterable(final SortedMap<ByteBuffer, Column> columnValues) {
            Preconditions.checkNotNull(columnValues);
            this.columnValues = columnValues;
        }

        @Override
        public Iterator<Entry> iterator() {
            return new CassandraMapIterator(columnValues.entrySet().iterator());
        }

    }

    private static class CassandraMapIterator implements Iterator<Entry> {

        private final Iterator<Map.Entry<ByteBuffer, Column>> iterator;

        public CassandraMapIterator(final Iterator<Map.Entry<ByteBuffer, Column>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry next() {
            final Map.Entry<ByteBuffer, Column> entry = iterator.next();
            ByteBuffer col = entry.getKey();
            ByteBuffer val = entry.getValue().value();
            return StaticArrayEntry.of(StaticArrayBuffer.of(col), StaticArrayBuffer.of(val));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
