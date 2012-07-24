package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapSequence {

    public static final String MAP_CLASSES = Tokens.makeNamespace(MapSequence.class) + ".mapClasses";

    public static class Map extends MemoryMapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private List<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>> mappers = new ArrayList<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>>();
        private List<Method> mapperMethods = new ArrayList<Method>();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                final MemoryMapContext memoryContext = new MemoryMapContext(context);
                final String[] classNames = context.getConfiguration().getStrings(MAP_CLASSES);
                for (int i = 0; i < classNames.length; i++) {
                    memoryContext.stageConfiguration(i);
                    final Class<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>> mapClass = (Class) Class.forName(classNames[i]);
                    final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapper = mapClass.getConstructor().newInstance();
                    try {
                        mapClass.getMethod(Tokens.SETUP, Mapper.Context.class).invoke(mapper, memoryContext);
                    } catch (NoSuchMethodException e) {
                        // there is no setup method and that is okay.
                    }
                    this.mappers.add(mapper);
                    this.mapperMethods.add(mapClass.getMethod(Tokens.MAP, NullWritable.class, FaunusVertex.class, Mapper.Context.class));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            try {
                final MemoryMapContext memoryContext = new MemoryMapContext(context);
                memoryContext.setCurrentValue(value);
                for (int i = 0; i < this.mappers.size(); i++) {
                    this.mapperMethods.get(i).invoke(this.mappers.get(i), key, memoryContext.getCurrentValue(), memoryContext);
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

    }
}
