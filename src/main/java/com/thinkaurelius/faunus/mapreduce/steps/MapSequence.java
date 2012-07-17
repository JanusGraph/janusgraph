package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapSequence {

    public static final String CLASSES = Tokens.makeNamespace(MapSequence.class) + ".classes";

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private final List<Class<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>>> mapSteps = new ArrayList<Class<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>>>();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                for (String classString : context.getConfiguration().getStrings(CLASSES)) {
                    this.mapSteps.add((Class) Class.forName(classString));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            try {
                final Context memoryContext = new Context(context);
                memoryContext.setCurrentValue(value);
                int step = 0;
                for (final Class<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>> mapStep : this.mapSteps) {
                    memoryContext.stageConfiguration(step++);
                    final Mapper mapper = mapStep.getConstructor().newInstance();
                    try {
                        mapStep.getMethod("setup", Mapper.Context.class).invoke(mapper, memoryContext);
                    } catch (NoSuchMethodException e) {

                    }
                    mapStep.getMethod("map", NullWritable.class, FaunusVertex.class, Mapper.Context.class).invoke(mapper, key, value, memoryContext);
                    if (null == memoryContext.getCurrentValue())
                        break;
                    memoryContext.reset();
                }
                final FaunusVertex vertex = memoryContext.getCurrentValue();
                if (null != vertex)
                    context.write(NullWritable.get(), vertex);
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        public class Context extends Mapper.Context {

            private FaunusVertex value;
            private Mapper.Context context;
            private boolean locked = false;
            private Configuration configuration;


            public Context(Mapper.Context context) throws IOException, InterruptedException {
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
                for (java.util.Map.Entry<String, String> entry : temp.entrySet()) {
                    this.configuration.set(entry.getKey(), entry.getValue());
                }
            }
        }

    }
}
