package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CloseTriangle {

    public static final String FIRST_DIRECTION = Tokens.makeNamespace(CloseTriangle.class) + ".firstDirection";
    public static final String FIRST_LABELS = Tokens.makeNamespace(CloseTriangle.class) + ".firstLabels";
    public static final String FIRST_ACTION = Tokens.makeNamespace(CloseTriangle.class) + ".firstAction";
    public static final String SECOND_DIRECTION = Tokens.makeNamespace(CloseTriangle.class) + ".secondDirection";
    public static final String SECOND_LABELS = Tokens.makeNamespace(CloseTriangle.class) + ".secondLabels";
    public static final String SECOND_ACTION = Tokens.makeNamespace(CloseTriangle.class) + ".secondAction";
    public static final String NEW_DIRECTION = Tokens.makeNamespace(CloseTriangle.class) + ".newDirection";
    public static final String NEW_LABEL = Tokens.makeNamespace(CloseTriangle.class) + ".newLabel";

    public enum Counters {
        EDGES_CREATED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction firstDirection;
        private String[] firstLabels;
        private Tokens.Action firstAction;
        private Direction secondDirection;
        private String[] secondLabels;
        private Tokens.Action secondAction;
        private Direction newDirection;
        private String newLabel;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.firstDirection = Direction.valueOf(context.getConfiguration().get(FIRST_DIRECTION));
            this.firstLabels = context.getConfiguration().getStrings(FIRST_LABELS, new String[0]);
            this.firstAction = Tokens.Action.valueOf(context.getConfiguration().get(FIRST_ACTION));
            this.secondDirection = Direction.valueOf(context.getConfiguration().get(SECOND_DIRECTION));
            this.secondLabels = context.getConfiguration().getStrings(SECOND_LABELS, new String[0]);
            this.secondAction = Tokens.Action.valueOf(context.getConfiguration().get(SECOND_ACTION));
            this.newDirection = Direction.valueOf(context.getConfiguration().get(NEW_DIRECTION));
            this.newLabel = context.getConfiguration().get(NEW_LABEL);
        }

        private final LongWritable longWritable = new LongWritable();
        private final Holder<FaunusEdge> edgeHolder = new Holder<FaunusEdge>();
        private final Holder<FaunusVertex> vertexHolder = new Holder<FaunusVertex>();
        private final FaunusEdge newEdge = new FaunusEdge();

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final Set<Long> firstVertexIds = new HashSet<Long>();
            final Set<Long> secondVertexIds = new HashSet<Long>();

            if (this.firstDirection.equals(OUT)) {
                for (final Edge edge : value.getEdges(IN, this.firstLabels)) {
                    firstVertexIds.add(((FaunusEdge) edge).getVertexId(OUT));
                }
            } else if (this.firstDirection.equals(IN)) {
                for (final Edge edge : value.getEdges(OUT, this.firstLabels)) {
                    firstVertexIds.add(((FaunusEdge) edge).getVertexId(IN));
                }
            } else {
                throw new IOException("A direction of " + this.firstDirection + " is not a legal direction for this operation");
            }

            if (this.secondDirection.equals(OUT)) {
                for (final Edge edge : value.getEdges(OUT, this.secondLabels)) {
                    secondVertexIds.add(((FaunusEdge) edge).getVertexId(IN));
                }
            } else if (this.secondDirection.equals(IN)) {
                for (final Edge edge : value.getEdges(IN, this.secondLabels)) {
                    secondVertexIds.add(((FaunusEdge) edge).getVertexId(OUT));
                }
            } else {
                throw new IOException("A direction of " + this.secondDirection + " is not a legal direction for this operation");
            }


            this.longWritable.set(value.getIdAsLong());
            if (this.firstAction.equals(Tokens.Action.DROP)) {
                value.removeEdges(Tokens.Action.DROP, Direction.BOTH, this.firstLabels);
            }
            if (this.secondAction.equals(Tokens.Action.DROP)) {
                value.removeEdges(Tokens.Action.DROP, Direction.BOTH, this.secondLabels);
            }

            context.write(this.longWritable, this.vertexHolder.set('v', value));

            for (final Long firstId : firstVertexIds) {
                for (final Long secondId : secondVertexIds) {
                    if (this.newDirection.equals(OUT)) {
                        this.newEdge.refill(-1l, firstId, secondId, this.newLabel);
                        this.longWritable.set(firstId);
                        context.write(this.longWritable, this.edgeHolder.set('o', this.newEdge));
                        this.longWritable.set(secondId);
                        context.write(this.longWritable, this.edgeHolder.set('i', this.newEdge));
                    } else {
                        this.newEdge.refill(-1l, secondId, firstId, this.newLabel);
                        this.longWritable.set(firstId);
                        context.write(this.longWritable, this.edgeHolder.set('i', this.newEdge));
                        this.longWritable.set(secondId);
                        context.write(this.longWritable, this.edgeHolder.set('o', this.newEdge));
                    }

                    context.getCounter(Counters.EDGES_CREATED).increment(2);
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {
        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    final FaunusVertex vertex2 = (FaunusVertex) holder.get();
                    vertex.setProperties(vertex2.getProperties());
                    // TODO: make this more efficient
                    for (final Edge edge : vertex2.getEdges(OUT)) {
                        vertex.addEdge(OUT, (FaunusEdge) edge);
                    }
                    for (final Edge edge : vertex2.getEdges(IN)) {
                        vertex.addEdge(IN, (FaunusEdge) edge);
                    }
                } else if (tag == 'i') {
                    vertex.addEdge(IN, (FaunusEdge) holder.get());
                } else if (tag == 'o') {
                    vertex.addEdge(OUT, (FaunusEdge) holder.get());
                } else {
                    throw new IOException("A tag of " + tag + " is not a legal tag for this operation");
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
