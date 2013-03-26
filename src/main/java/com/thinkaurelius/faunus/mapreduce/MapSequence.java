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
        private int size = 0;
        private MemoryMapContext memoryContext;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            if (this.mappers.size() == 0) {
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
                            } catch (final NoSuchMethodException e) {
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
                            } catch (final NoSuchMethodException e) {
                                this.cleanupMethods.add(null);
                            }
                        }
                    }
                    this.size = this.mappers.size();
                    this.memoryContext = new MemoryMapContext(context);
                } catch (final Exception e) {
                    throw new IOException(e);
                }
            }
        }

        @Override
        public void map(final Writable key, final Writable value, final Mapper<Writable, Writable, Writable, Writable>.Context context) throws IOException, InterruptedException {
            try {
                this.memoryContext.setContext(context);
                this.memoryContext.write(key, value);

                for (int i = 0; i < this.size - 1; i++) {
                    this.mapMethods.get(i).invoke(this.mappers.get(i), this.memoryContext.getCurrentKey(), this.memoryContext.getCurrentValue(), this.memoryContext);
                    if (!this.memoryContext.nextKeyValue()) {
                        break;
                    }
                }

                if (this.memoryContext.nextKeyValue()) {
                    this.mapMethods.get(this.size - 1).invoke(this.mappers.get(this.size - 1), this.memoryContext.getCurrentKey(), this.memoryContext.getCurrentValue(), context);
                }

            } catch (final Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void cleanup(final Mapper<Writable, Writable, Writable, Writable>.Context context) throws IOException, InterruptedException {
            try {
                for (int i = 0; i < this.mappers.size(); i++) {
                    final Method cleanup = this.cleanupMethods.get(i);
                    if (null != cleanup)
                        cleanup.invoke(this.mappers.get(i), context);
                }
            } catch (final Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

    }
}
