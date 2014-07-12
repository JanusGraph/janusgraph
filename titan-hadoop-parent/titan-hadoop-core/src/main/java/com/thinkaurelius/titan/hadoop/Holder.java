package com.thinkaurelius.titan.hadoop;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T extends FaunusPathElement> extends GenericWritable implements WritableComparable<Holder<T>> {

    protected char tag;

    static {
        WritableComparator.define(Holder.class, new Comparator());
    }

    private static Class[] CLASSES = {
            FaunusVertex.class,
            StandardFaunusEdge.class
    };

    protected Class<T>[] getTypes() {
        return CLASSES;

    }

    public Holder() {
        super();
    }

    public Holder(final DataInput in) throws IOException {
        this();
        this.readFields(in);
    }

    public Holder(final char tag, final T element) {
        this();
        this.set(element);
        this.tag = tag;
    }

    @Override
    public int hashCode() {
        return super.get().hashCode();
    }

    public char getTag() {
        return this.tag;
    }

    public T get() {
        return (T) super.get();
    }

    public Holder<T> set(final char tag, final T element) {
        this.set(element);
        this.tag = tag;
        return this;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeChar(this.tag);
        super.write(out);

    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.tag = in.readChar();
        super.readFields(in);
    }

    @Override
    public boolean equals(final Object object) {
        return object.getClass().equals(Holder.class) && ((Holder) object).getTag() == this.tag && ((Holder) object).get().equals(this.get());
    }

    @Override
    public int compareTo(final Holder<T> holder) {
        final FaunusElement e1 = holder.get();
        final FaunusElement e2 = this.get();
        if (e1 instanceof FaunusVertex && e2 instanceof FaunusVertex)
            return e1.compareTo(e2);
        else
            return 0;
    }


    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(Holder.class);
        }

        @Override
        public int compare(final byte[] holder1, final int start1, final int length1, final byte[] holder2, final int start2, final int length2) {
            // 1 byte is the class
            // 1 byte is the character
            // the next vlong bytes are the long id
            try {
                return Long.valueOf(readVLong(holder1, 3)).compareTo(readVLong(holder2, 3));
            } catch (IOException e) {
                return -1;
            }
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof Holder && b instanceof Holder)
                return ((Long) (((Holder) a).get().getLongId())).compareTo(((Holder) b).get().getLongId());
            else
                return super.compare(a, b);
        }
    }
}
