package com.thinkaurelius.faunus.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapper<A, B, C, D> extends Mapper<A, B, C, D> {

    public class MemoryMapContext extends Mapper.Context {

        private static final String DASH = "-";
        private static final String EMPTY = "";

        private final Configuration currentConfiguration = new Configuration();

        private Queue<Writable> keys = new LinkedList<Writable>();
        private Queue<Writable> values = new LinkedList<Writable>();
        private Mapper.Context context;
        private Configuration globalConfiguration;

        public MemoryMapContext(final Mapper.Context context) throws IOException, InterruptedException {
            super(context.getConfiguration(), context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, context.getInputSplit());
            this.context = context;
            this.globalConfiguration = context.getConfiguration();
        }

        @Override
        public void write(final Object key, final Object value) throws IOException, InterruptedException {
            this.keys.add((Writable) key);
            this.values.add((Writable) value);
        }

        @Override
        public Writable getCurrentKey() {
            return this.keys.remove();
        }

        @Override
        public Writable getCurrentValue() {
            return this.values.remove();
        }

        @Override
        public boolean nextKeyValue() {
            return !this.keys.isEmpty() && !this.values.isEmpty();
        }

        @Override
        public Counter getCounter(final String groupName, final String counterName) {
            return this.context.getCounter(groupName, counterName);
        }

        @Override
        public Counter getCounter(final Enum counterName) {
            return this.context.getCounter(counterName);
        }

        @Override
        public Configuration getConfiguration() {
            return this.currentConfiguration;
        }

        public void stageConfiguration(final int step) {
            this.currentConfiguration.clear();
            for (final Map.Entry<String, String> entry : this.globalConfiguration) {
                final String key = entry.getKey();
                if (key.endsWith(DASH + step)) {
                    this.currentConfiguration.set(key.replace(DASH + step, EMPTY), entry.getValue());
                } else if (!key.matches(".*-[0-9]+")) {
                    this.currentConfiguration.set(key, entry.getValue());
                }
            }
        }
    }
}
