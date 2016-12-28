package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Wraps a ColumnFamilyRecordReader and converts CFRR's binary types to Titan binary types.
 */
public class CassandraBinaryRecordReader extends RecordReader<StaticBuffer, Iterable<Entry>> {

    private ColumnFamilyRecordReader reader;

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
                SortedMap<ByteBuffer, Cell> valueSortedMap = reader.getCurrentValue();
                List<Entry> entries = new ArrayList<>(valueSortedMap.size());
                for (Map.Entry<ByteBuffer, Cell> ent : valueSortedMap.entrySet()) {
                    ByteBuffer col = ent.getKey();
                    ByteBuffer val = ent.getValue().value();
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
