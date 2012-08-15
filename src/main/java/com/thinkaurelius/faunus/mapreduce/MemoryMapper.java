package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapper<A, B, C, D> extends Mapper<A, B, C, D> {

    public class MemoryMapContext extends Mapper.Context {

        private static final String DASH = "-";
        private static final String EMPTY = "";

        private final Configuration currentConfiguration = new Configuration();

        private FaunusVertex value;
        private Mapper.Context context;
        private boolean locked = false;
        private Configuration globalConfiguration;
        private boolean wasWritten = false;

        public MemoryMapContext(final Mapper.Context context) throws IOException, InterruptedException {
            super(context.getConfiguration(), context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, context.getInputSplit());
            this.context = context;
            this.globalConfiguration = context.getConfiguration();
        }

        @Override
        public void write(final Object key, final Object value) throws IOException, InterruptedException {
            this.value = (FaunusVertex) value;
            this.wasWritten = true;
        }

        @Override
        public FaunusVertex getCurrentValue() {
            return this.value;
        }

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public boolean nextKeyValue() {
            if (this.locked)
                return false;
            else {
                this.locked = true;
                return true;
            }
        }

        public boolean wasWritten() {
            return this.wasWritten;
        }

        public void setWasWritten(final boolean wasWritten) {
            this.wasWritten = wasWritten;
        }

        public void reset() {
            this.locked = false;
        }

        public void setCurrentValue(final FaunusVertex value) {
            this.value = value;
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

        public void stageConfiguration() {
            this.currentConfiguration.clear();
            for (final Map.Entry<String, String> entry : this.globalConfiguration) {
                final String key = entry.getKey();
                if (!key.matches(".*-[0-9]+")) {
                    this.currentConfiguration.set(key, entry.getValue());
                }
            }
        }
    }
}
