package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapper<A, B, C, D> extends Mapper<A, B, C, D> {

    public class MemoryMapContext extends Mapper.Context {

        private FaunusVertex value;
        private Mapper.Context context;
        private boolean locked = false;
        private Configuration configuration;


        public MemoryMapContext(final Mapper.Context context) throws IOException, InterruptedException {
            super(context.getConfiguration(), context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, context.getInputSplit());
            this.context = context;
            this.configuration = context.getConfiguration();
        }

        @Override
        public void write(final Object key, final Object value) throws IOException, InterruptedException {
            this.value = (FaunusVertex) value;
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
            return this.configuration;
        }

        public void stageConfiguration(final int step) {
            final java.util.Map<String, String> temp = new HashMap<String, String>();
            for (final java.util.Map.Entry<String, String> entry : this.configuration) {
                final String key = entry.getKey();
                if (key.endsWith("-" + step)) {
                    temp.put(key.replace("-" + step, ""), entry.getValue());
                }
            }
            for (final java.util.Map.Entry<String, String> entry : temp.entrySet()) {
                this.configuration.set(entry.getKey(), entry.getValue());
            }
        }
    }
}
