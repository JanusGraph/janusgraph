package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.io.Writable;
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

    public static class Map extends MemoryMapper<Writable, Writable, Writable, Writable> {

        private List<Mapper<Writable, Writable, Writable, Writable>> mappers = new ArrayList<Mapper<Writable, Writable, Writable, Writable>>();
        private List<Method> mapMethods = new ArrayList<Method>();
        private List<Method> cleanupMethods = new ArrayList<Method>();
        //private MultipleOutputs outputs;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            if (mappers.size() == 0)
                try {
                    final MemoryMapContext memoryContext = new MemoryMapContext(context);
                    final String[] mapClassNames = context.getConfiguration().getStrings(MAP_CLASSES, new String[0]);
                    if (mapClassNames.length > 0) {
                        for (int i = 0; i < mapClassNames.length; i++) {
                            memoryContext.stageConfiguration(i);
                            final Class<Mapper<Writable, Writable, Writable, Writable>> mapClass = (Class) Class.forName(mapClassNames[i]);
                            final Mapper<Writable, Writable, Writable, Writable> mapper = mapClass.getConstructor().newInstance();
                            try {
                                mapClass.getMethod(Tokens.SETUP, Mapper.Context.class).invoke(mapper, memoryContext);
                            } catch (NoSuchMethodException e) {
                                // there is no setup method and that is okay.
                            }
                            this.mappers.add(mapper);
                            for (final Method method : mapClass.getMethods()) {
                                if (method.getName().equals(Tokens.MAP)) {
                                    this.mapMethods.add(method);
                                    break;
                                }
                            }
                            try {
                                this.cleanupMethods.add(mapClass.getMethod(Tokens.CLEANUP, Mapper.Context.class));
                            } catch (NoSuchMethodException e) {
                                this.cleanupMethods.add(null);
                            }

                        }
                    }
                    //this.outputs = new MultipleOutputs(context);
                } catch (Exception e) {
                    throw new IOException(e);
                }
        }


        @Override
        public void map(final Writable key, final Writable value, final Mapper<Writable, Writable, Writable, Writable>.Context context) throws IOException, InterruptedException {
            try {
                final int size = this.mappers.size();
                //System.out.println(this.mappers.size());
                Writable currentKey = key;
                Writable currentValue = value;

                final MemoryMapContext memoryContext = new MemoryMapContext(context);
                for (int i = 0; i < size - 1; i++) {
                    final Mapper mapper = this.mappers.get(i);
                    final Method map = this.mapMethods.get(i);
                    final Method cleanup = this.cleanupMethods.get(i);

                    map.invoke(mapper, currentKey, currentValue, memoryContext);
                    if (!memoryContext.nextKeyValue()) {
                        currentKey = null;
                        currentValue = null;
                        break;
                    } else {
                        currentKey = memoryContext.getCurrentKey();
                        currentValue = memoryContext.getCurrentValue();
                    }
                    if (null != cleanup)
                        cleanup.invoke(mapper, memoryContext);
                }

                if (currentKey != null && currentValue != null) {
                    final Mapper mapper = this.mappers.get(size - 1);
                    final Method map = this.mapMethods.get(size - 1);
                    map.invoke(mapper, currentKey, currentValue, context);
                }

            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void cleanup(final Mapper<Writable, Writable, Writable, Writable>.Context context) throws IOException, InterruptedException {
            try {
                final int size = this.mappers.size();
                final Mapper mapper = this.mappers.get(size - 1);
                final Method cleanup = this.cleanupMethods.get(size - 1);
                if (null != cleanup)
                    cleanup.invoke(mapper, context);
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

    }
}
