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
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Wraps a ColumnFamilyRecordReader and converts CFRR's binary types to JanusGraph binary types.
 */
public class CassandraBinaryRecordReader extends RecordReader<StaticBuffer, Iterable<Entry>> {

    private final ColumnFamilyRecordReader reader;

    private KV currentKV;
    private KV incompleteKV;

    public CassandraBinaryRecordReader(final ColumnFamilyRecordReader reader) {
        this.reader = reader;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return null != (currentKV = completeNextKV());
    }

    private KV completeNextKV() throws IOException {
        KV completedKV = null;
        boolean hasNext;
        do {
            hasNext = reader.nextKeyValue();

            if (!hasNext) {
                completedKV = incompleteKV;
                incompleteKV = null;
            } else {
                StaticArrayBuffer key = StaticArrayBuffer.of(reader.getCurrentKey());
                SortedMap<ByteBuffer, ColumnFamilyRecordReader.Column> valueSortedMap = reader.getCurrentValue();
                List<Entry> entries = new ArrayList<>(valueSortedMap.size());
                for (Map.Entry<ByteBuffer, ColumnFamilyRecordReader.Column> ent : valueSortedMap.entrySet()) {
                    ByteBuffer col = ent.getKey();
                    ByteBuffer val = ent.getValue().value;
                    entries.add(StaticArrayEntry.of(StaticArrayBuffer.of(col), StaticArrayBuffer.of(val)));
                }

                if (null == incompleteKV) {
                    // Initialization; this should happen just once in an instance's lifetime
                    incompleteKV = new KV(key);
                } else if (!incompleteKV.key.equals(key)) {
                    // The underlying Cassandra reader has just changed to a key we haven't seen yet
                    // This implies that there will be no more entries for the prior key
                    completedKV = incompleteKV;
                    incompleteKV = new KV(key);
                }

                incompleteKV.addEntries(entries);
            }
            /* Loop ends when either
             * A) the cassandra reader ran out of data
             * or
             * B) the cassandra reader switched keys, thereby completing a KV */
        } while (hasNext && null == completedKV);

        return completedKV;
    }

    @Override
    public StaticBuffer getCurrentKey() throws IOException, InterruptedException {
        return currentKV.key;
    }

    @Override
    public Iterable<Entry> getCurrentValue() throws IOException, InterruptedException {
        return currentKV.entries;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public float getProgress() {
        return reader.getProgress();
    }

    private static class KV {
        private final StaticArrayBuffer key;
        private ArrayList<Entry> entries;

        public KV(StaticArrayBuffer key) {
            this.key = key;
        }

        public void addEntries(Collection<Entry> toAdd) {
            if (null == entries)
                entries = new ArrayList<>(toAdd.size());

            entries.addAll(toAdd);
        }
    }
}
