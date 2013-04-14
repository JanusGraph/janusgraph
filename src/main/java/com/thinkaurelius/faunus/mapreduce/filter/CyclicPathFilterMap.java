package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CyclicPathFilterMap {

    public static final String CLASS = Tokens.makeNamespace(CyclicPathFilterMap.class) + ".class";

    public enum Counters {
        PATHS_FILTERED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.setBoolean(FaunusCompiler.PATH_ENABLED, true);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean isVertex;
        private HashSet set = new HashSet();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            long pathsFiltered = 0l;
            if (this.isVertex) {
                if (value.hasPaths()) {
                    final Iterator<List<FaunusElement.MicroElement>> itty = value.getPaths().iterator();
                    while (itty.hasNext()) {
                        final List<FaunusElement.MicroElement> path = itty.next();
                        this.set.clear();
                        this.set.addAll(path);
                        if (path.size() != this.set.size()) {
                            itty.remove();
                            pathsFiltered++;
                        }
                    }
                }
            } else {
                for (final Edge e : value.getEdges(Direction.BOTH)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        final Iterator<List<FaunusElement.MicroElement>> itty = edge.getPaths().iterator();
                        while (itty.hasNext()) {
                            final List<FaunusElement.MicroElement> path = itty.next();
                            this.set.clear();
                            this.set.addAll(path);
                            if (path.size() != this.set.size()) {
                                itty.remove();
                                pathsFiltered++;
                            }
                        }
                    }
                }
            }

            context.getCounter(Counters.PATHS_FILTERED).increment(pathsFiltered);
            context.write(NullWritable.get(), value);
        }
    }
}
