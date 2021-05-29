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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseBinaryRecordReader  extends RecordReader<StaticBuffer, Iterable<Entry>> {

    private final RecordReader<ImmutableBytesWritable, Result> reader;

    private final byte[] edgestoreFamilyBytes;

    public HBaseBinaryRecordReader(final RecordReader<ImmutableBytesWritable, Result> reader, final byte[] edgestoreFamilyBytes) {
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
    public float getProgress() throws IOException, InterruptedException {
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

