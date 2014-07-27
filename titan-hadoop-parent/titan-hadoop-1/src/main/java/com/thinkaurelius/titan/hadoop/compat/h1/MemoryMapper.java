package com.thinkaurelius.titan.hadoop.compat.h1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.util.Map;

/**
 * MemoryMapper supports in-memory mapping for a chain of consecutive mappers.
 * This provides significant performance improvements as each map need not write its results to disk.
 * Note that MemoryMapper is not general-purpose and is specific to Titan/Hadoop's current MapReduce library.
 * In particular, it assumes that the chain of mappers emits 0 or 1 key/value pairs for each input.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapper<A, B, C, D> extends Mapper<A, B, C, D> {

    public class MemoryMapContext extends Mapper.Context {

        private static final String DASH = "-";
        private static final String EMPTY = "";

        private final Configuration currentConfiguration = new Configuration();

        private Writable key = null;
        private Writable value = null;
        private Writable tempKey = null;
        private Writable tempValue = null;
        private Mapper.Context context;
        private Configuration globalConfiguration;

        public MemoryMapContext(final Mapper.Context context) throws IOException, InterruptedException {
            super(context.getConfiguration(), context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, context.getInputSplit());
            this.context = context;
            this.globalConfiguration = context.getConfiguration();
        }

        @Override
        public void write(final Object key, final Object value) throws IOException, InterruptedException {
            this.key = (Writable) key;
            this.value = (Writable) value;
        }

        @Override
        public Writable getCurrentKey() {
            this.tempKey = this.key;
            this.key = null;
            return this.tempKey;
        }

        @Override
        public Writable getCurrentValue() {
            this.tempValue = this.value;
            this.value = null;
            return tempValue;
        }

        @Override
        public boolean nextKeyValue() {
            return this.key != null && this.value != null;
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

        public void setContext(final Mapper.Context context) {
            this.context = context;
        }

        public void stageConfiguration(final int step) {
            this.currentConfiguration.clear();
            for (final Map.Entry<String, String> entry : this.globalConfiguration) {
                final String key = entry.getKey();
                if (!key.matches(".*-[0-9]+")) {
                    this.currentConfiguration.set(key, entry.getValue());
                }
            }
            for (final Map.Entry<String, String> entry : this.globalConfiguration) {
                final String key = entry.getKey();
                if (key.endsWith(DASH + step)) {
                    this.currentConfiguration.set(key.replace(DASH + step, EMPTY), entry.getValue());
                }
            }
        }
    }
}
