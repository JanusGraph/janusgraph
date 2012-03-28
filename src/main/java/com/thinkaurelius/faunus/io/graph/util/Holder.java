package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T extends FaunusElement> extends GenericWritable implements WritableComparable<Holder<T>>, RawComparator<Holder<T>> {

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

    @Override
    public int compare(final Holder<T> holder1, final Holder<T> holder2) {
        return ((Long) holder1.get().getId()).compareTo((Long) holder2.get().getId());
    }

    @Override
    public int compare(byte[] holder1, int start1, int length1, byte[] holder2, int start2, int length2) {
        // first byte is the class type
        // second byte is the element type
        // the next 8 bytes are the long id
        if (holder1[1] != holder2[1]) {
            return new Byte(holder1[1]).compareTo(holder2[1]);
        }

        final Long id1 = ByteBuffer.wrap(holder1, 2, 10).getLong();
        final Long id2 = ByteBuffer.wrap(holder2, 2, 10).getLong();

        return id1.compareTo(id2);
    }
}
