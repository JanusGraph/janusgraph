package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapReduceSequence {

    public static final String MAP_CLASSES = Tokens.makeNamespace(MapSequence.class) + ".mapClasses";
    public static final String MAPR_CLASS = Tokens.makeNamespace(MapSequence.class) + ".mapRClass";
    public static final String REDUCE_CLASS = Tokens.makeNamespace(MapSequence.class) + ".reduceClass";

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private List<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>> mappers = new ArrayList<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>>();
        private List<Method> mapperMethods = new ArrayList<Method>();
        private Mapper<NullWritable, FaunusVertex, LongWritable, Holder> mapperR;
        private Method mapperRMethod;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                final MemoryMapContext memoryContext = new MemoryMapContext(context);
                final String[] mapClassNames = context.getConfiguration().getStrings(MAP_CLASSES);
                if (null != mapClassNames && mapClassNames.length > 0) {
                    for (int i = 0; i < mapClassNames.length; i++) {
                        memoryContext.stageConfiguration(i);
                        final Class<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>> mapClass = (Class) Class.forName(mapClassNames[i]);
                        final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapper = mapClass.getConstructor().newInstance();
                        try {
                            mapClass.getMethod(Tokens.SETUP, Mapper.Context.class).invoke(mapper, memoryContext);
                        } catch (NoSuchMethodException e) {
                            // there is no setup method and that is okay.
                        }
                        this.mappers.add(mapper);
                        this.mapperMethods.add(mapClass.getMethod(Tokens.MAP, NullWritable.class, FaunusVertex.class, Mapper.Context.class));
                    }
                }
                final String mapRClassName = context.getConfiguration().get(MAPR_CLASS);
                final Class<Mapper<NullWritable, FaunusVertex, LongWritable, Holder>> mapRClass = (Class) Class.forName(mapRClassName);
                this.mapperR = mapRClass.getConstructor().newInstance();
                try {
                    mapRClass.getMethod(Tokens.SETUP, Mapper.Context.class).invoke(this.mapperR, memoryContext);
                } catch (NoSuchMethodException e) {
                    // there is no setup method and that is okay.
                }
                this.mapperRMethod = mapRClass.getMethod(Tokens.MAP, NullWritable.class, FaunusVertex.class, Mapper.Context.class);

            } catch (Exception e) {
                throw new IOException(e);
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            try {
                FaunusVertex vertex = value;
                if (this.mappers.size() > 0) {
                    final MemoryMapContext memoryContext = new MemoryMapContext(context);
                    memoryContext.setCurrentValue(value);
                    for (int i = 0; i < this.mappers.size(); i++) {
                        final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapper = this.mappers.get(i);
                        this.mapperMethods.get(i).invoke(mapper, key, vertex, memoryContext);
                        vertex = memoryContext.getCurrentValue();
                        if (null == vertex)
                            break;
                        memoryContext.reset();
                    }
                }
                if (null != vertex) {
                    this.mapperRMethod.invoke(this.mapperR, key, vertex, context);
                }
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

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

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Reducer<LongWritable, Holder, NullWritable, FaunusVertex> reducer;
        private Method reducerMethod;


        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            try {
                Class<Reducer<LongWritable, Holder, NullWritable, FaunusVertex>> reducerClass = (Class) Class.forName(context.getConfiguration().get(REDUCE_CLASS));
                this.reducer = reducerClass.getConstructor().newInstance();
                try {
                    reducerClass.getMethod(Tokens.SETUP, Reducer.Context.class).invoke(this.reducer, context);
                } catch (NoSuchMethodException e) {
                    // there is no setup method and that is okay.
                }
                this.reducerMethod = reducerClass.getMethod(Tokens.REDUCE, LongWritable.class, Iterable.class, Reducer.Context.class);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            try {
                this.reducerMethod.invoke(this.reducer, key, values, context);
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }


}
