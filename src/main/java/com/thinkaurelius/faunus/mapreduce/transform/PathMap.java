package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.faunus.mapreduce.util.SafeMapperOutputs;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathMap {

    public static final String CLASS = Tokens.makeNamespace(PathMap.class) + ".class";

    public enum Counters {
        VERTICES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.setBoolean(FaunusCompiler.PATH_ENABLED, true);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, Text> {

        private boolean isVertex;
        private final Text textWritable = new Text();
        private SafeMapperOutputs outputs;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.outputs = new SafeMapperOutputs(context);
            if (!context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false))
                throw new IllegalStateException(PathMap.class.getSimpleName() + " requires that paths be enabled");
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            if (this.isVertex && value.hasPaths()) {
                for (final List<FaunusElement.MicroElement> path : value.getPaths()) {
                    this.textWritable.set(path.toString());
                    this.outputs.write(Tokens.SIDEEFFECT, NullWritable.get(), this.textWritable);
                }
                context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        for (final List<FaunusElement.MicroElement> path : edge.getPaths()) {
                            this.textWritable.set(path.toString());
                            this.outputs.write(Tokens.SIDEEFFECT, NullWritable.get(), this.textWritable);
                        }
                        edgesProcessed++;
                    }
                }
                context.getCounter(Counters.OUT_EDGES_PROCESSED).increment(edgesProcessed);
            }

            this.outputs.write(Tokens.GRAPH, NullWritable.get(), value);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }
    }
}
