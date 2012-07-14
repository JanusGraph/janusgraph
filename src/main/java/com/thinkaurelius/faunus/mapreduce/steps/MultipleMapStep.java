package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MultipleMapStep {

    public static final String CLASSES = Tokens.makeNamespace(RetainEdgeLabels.class) + ".classes";

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
               // FaunusVertex vertex = value;
                for (Class<Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>> step : mapSteps) {

                    Method method = step.getMethod("map", NullWritable.class, FaunusVertex.class, Mapper.Context.class);
                    method.invoke(step.getConstructor().newInstance(), key, value, context);
                    
                }
               // context.write(NullWritable.get(), vertex);
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

    }


}
