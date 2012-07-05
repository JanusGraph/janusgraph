package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T extends FaunusElement> extends GenericWritable implements WritableComparable<Holder<T>> {

    static {
        WritableComparator.define(Holder.class, new Comparator());
    }

    private static Class[] CLASSES = {
            FaunusVertex.class,
            FaunusEdge.class
    };

    protected Class[] getTypes() {
        return CLASSES;

    }

    public Holder() {
        super();
    }

    public Holder(final T element) {
        this.set(element);
    }

    public Holder(final DataInput in) throws IOException {
        this.readFields(in);
    }

    public T get() {
        return (T) super.get();
    }

    @Override
    public int compareTo(Holder<T> holder) {
        return holder.get().compareTo(this.get());
    }

    @Override
    public boolean equals(final Object object) {
        return object.getClass().equals(Holder.class) && ((Holder) object).get().equals(this.get());
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(Holder.class);
        }

        @Override
        public int compare(final byte[] holder1, final int start1, final int length1, final byte[] holder2, final int start2, final int length2) {
            // 0 byte is the class type
            // 1 byte is the element type
            // the next 8 bytes are the long id
            final ByteBuffer buffer1 = ByteBuffer.wrap(holder1);
            final ByteBuffer buffer2 = ByteBuffer.wrap(holder2);

            buffer1.get();
            buffer2.get();

            final Byte type1 = buffer1.get();
            final Byte type2 = buffer2.get();
            if (!type1.equals(type2)) {
                return type1.compareTo(type2);
            }
            return (((Long) buffer1.getLong()).compareTo(buffer2.getLong()));
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof Holder && b instanceof Holder)
                return (((Holder) a).get().getIdAsLong()).compareTo(((Holder) b).get().getIdAsLong());
            else
                return super.compare(a, b);
        }
    }
}
