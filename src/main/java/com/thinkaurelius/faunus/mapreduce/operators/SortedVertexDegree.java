package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SortedVertexDegree {

    public static final String LABELS = Tokens.makeNamespace(SortedVertexDegree.class) + ".labels";
    public static final String DIRECTION = Tokens.makeNamespace(SortedVertexDegree.class) + ".direction";
    public static final String PROPERTY = Tokens.makeNamespace(SortedVertexDegree.class) + ".property";
    public static final String ORDER = Tokens.makeNamespace(SortedVertexDegree.class) + ".order";

    private static final String NULL = "null";

    public enum Counters {
        EDGES_COUNTED,
        VERTICES_COUNTED
    }

    public enum Order {
        STANDARD,
        REVERSE
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, DegreeHolder, NullWritable> {

        private Direction direction;
        private String[] labels;
        private Order order;


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.order = Order.valueOf(context.getConfiguration().get(ORDER));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, DegreeHolder, NullWritable>.Context context) throws IOException, InterruptedException {
            int degree = ((List<Edge>) value.getEdges(this.direction, this.labels)).size();
            context.getCounter(Counters.VERTICES_COUNTED).increment(1);
            context.getCounter(Counters.EDGES_COUNTED).increment(degree);
            if (this.order.equals(Order.REVERSE))
                context.write(new DegreeHolder(degree, value, 'r'), NullWritable.get());
            else
                context.write(new DegreeHolder(degree, value, 's'), NullWritable.get());
        }

    }

    public static class Reduce extends Reducer<DegreeHolder, NullWritable, Text, IntWritable> {

        private String property;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.property = context.getConfiguration().get(PROPERTY);
        }

        @Override
        public void reduce(final DegreeHolder key, final Iterable<NullWritable> values, final Reducer<DegreeHolder, NullWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            if (this.property.equals(Tokens._ID))
                context.write(new Text(key.get().getId().toString()), new IntWritable(key.getDegree()));
            else if (this.property.equals(Tokens._PROPERTIES))
                context.write(new Text(key.get().getProperties().toString()), new IntWritable(key.getDegree()));
            else {
                final Object property = key.get().getProperty(this.property);
                if (null != property)
                    context.write(new Text(property.toString()), new IntWritable(key.getDegree()));
                else
                    context.write(new Text(NULL), new IntWritable(key.getDegree()));
            }
        }
    }


    public static class DegreeHolder extends GenericWritable implements WritableComparable<DegreeHolder> {

        protected int degree;
        protected char order;

        static {
            WritableComparator.define(DegreeHolder.class, new Comparator());
        }

        private static Class[] CLASSES = {
                FaunusVertex.class,
        };

        protected Class<FaunusVertex>[] getTypes() {
            return CLASSES;

        }

        public DegreeHolder() {
            super();
        }

        public DegreeHolder(final int degree, final FaunusVertex vertex, final char order) {
            this();
            this.set(vertex);
            this.degree = degree;
            this.order = order;
        }

        public int getDegree() {
            return this.degree;
        }

        public FaunusVertex get() {
            return (FaunusVertex) super.get();
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeInt(this.degree);
            out.writeChar(this.order);
            super.write(out);

        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            this.degree = in.readInt();
            this.order = in.readChar();
            super.readFields(in);
        }

        @Override
        public boolean equals(final Object object) {
            return object.getClass().equals(DegreeHolder.class) && ((DegreeHolder) object).get().equals(this.get());
        }

        @Override
        public int compareTo(final DegreeHolder holder) {
            int otherDegree = holder.getDegree();

            if (this.degree > otherDegree)
                return this.order == 'r' ? -1 : 1;
            else if (this.degree < otherDegree)
                return this.order == 'r' ? 1 : -1;
            else
                return this.order == 'r' ? this.get().compareTo(holder.get()) : -1 * this.get().compareTo(holder.get());
        }


        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(DegreeHolder.class);
            }

            @Override
            public int compare(final byte[] holder1, final int start1, final int length1, final byte[] holder2, final int start2, final int length2) {
                // 1 byte is the class
                // 2 byte is the integer degree
                // the next 8 bytes are the long id

                final ByteBuffer buffer1 = ByteBuffer.wrap(holder1);
                final ByteBuffer buffer2 = ByteBuffer.wrap(holder2);

                buffer1.get();
                buffer2.get();

                int degree1 = buffer1.getInt();
                int degree2 = buffer2.getInt();
                char order = buffer1.getChar();

                if (degree1 > degree2)
                    return order == 'r' ? -1 : 1;
                else if (degree1 < degree2)
                    return order == 'r' ? 1 : -1;
                else
                    return order == 'r' ? ((Long) buffer1.getLong()).compareTo(buffer2.getLong()) : -1 * ((Long) buffer1.getLong()).compareTo(buffer2.getLong());
            }

            @Override
            public int compare(final WritableComparable a, final WritableComparable b) {
                if (a instanceof DegreeHolder && b instanceof DegreeHolder)
                    return ((DegreeHolder) a).compareTo((DegreeHolder) b);
                else
                    return super.compare(a, b);
            }
        }
    }
}
