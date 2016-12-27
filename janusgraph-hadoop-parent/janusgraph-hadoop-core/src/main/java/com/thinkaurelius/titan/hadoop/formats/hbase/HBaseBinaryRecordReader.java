package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseBinaryRecordReader  extends RecordReader<StaticBuffer, Iterable<Entry>> {

    private TableRecordReader reader;

    private final byte[] edgestoreFamilyBytes;

    public HBaseBinaryRecordReader(final TableRecordReader reader, final byte[] edgestoreFamilyBytes) {
        this.reader = reader;
        this.edgestoreFamilyBytes = edgestoreFamilyBytes;
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
        return StaticArrayBuffer.of(reader.getCurrentKey().copyBytes());
    }

    @Override
    public Iterable<Entry> getCurrentValue() throws IOException, InterruptedException {
        return new HBaseMapIterable(reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    @Override
    public float getProgress() {
        return this.reader.getProgress();
    }

    private static class HBaseMapIterable implements Iterable<Entry> {

        private final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues;

        public HBaseMapIterable(final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues) {
            Preconditions.checkNotNull(columnValues);
            this.columnValues = columnValues;
        }

        @Override
        public Iterator<Entry> iterator() {
            return new HBaseMapIterator(columnValues.entrySet().iterator());
        }

    }

    private static class HBaseMapIterator implements Iterator<Entry> {

        private final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator;

        public HBaseMapIterator(final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry next() {
            final Map.Entry<byte[], NavigableMap<Long, byte[]>> entry = iterator.next();
            byte[] col = entry.getKey();
            byte[] val = entry.getValue().lastEntry().getValue();
            return StaticArrayEntry.of(new StaticArrayBuffer(col), new StaticArrayBuffer(val));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

