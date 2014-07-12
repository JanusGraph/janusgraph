package com.thinkaurelius.titan.hadoop;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WeightedWritable<T extends WritableComparable> extends GenericWritable implements WritableComparable<WeightedWritable<T>> {

    protected long weight;

    static {
        WritableComparator.define(WeightedWritable.class, new Comparator());
    }

    private static Class[] CLASSES = {
            FaunusVertex.class,
            StandardFaunusEdge.class,
            Text.class
    };

    protected Class<T>[] getTypes() {
        return CLASSES;

    }

    public WeightedWritable() {
        super();
    }

    public WeightedWritable(final DataInput in) throws IOException {
        this();
        this.readFields(in);
    }

    public WeightedWritable(final long weight, final T element) {
        this();
        this.set(element);
        this.weight = weight;
    }

    @Override
    public int hashCode() {
        return super.get().hashCode();
    }

    public long getWeight() {
        return this.weight;
    }

    public T get() {
        return (T) super.get();
    }

    public WeightedWritable<T> set(final long weight, final T writable) {
        this.set(writable);
        this.weight = weight;
        return this;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeLong(this.weight);
        super.write(out);

    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.weight = in.readLong();
        super.readFields(in);
    }

    @Override
    public boolean equals(final Object object) {
        // TODO: what is equality?
        return object.getClass().equals(WeightedWritable.class) && ((WeightedWritable) object).get().equals(this.get());
    }

    @Override
    public int compareTo(final WeightedWritable<T> writable) {
        return writable.get().compareTo(this.get());
    }

    public String toString() {
        return "[" + this.weight + "," + super.get() + "]";
    }


    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(WeightedWritable.class);
        }

       /* @Override
        public int compare(final byte[] holder1, final int start1, final int length1, final byte[] holder2, final int start2, final int length2) {
            // 1 byte is the class
            // 1 byte is the character
            // the next vlong bytes are the long id
            try {
                return Long.valueOf(readVLong(holder1, 3)).compareTo(readVLong(holder2, 3));
            } catch (IOException e) {
                return -1;
            }
        }*/

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof WeightedWritable && b instanceof WeightedWritable)
                return ((WeightedWritable) a).get().compareTo(((WeightedWritable) b).get());
            else
                return super.compare(a, b);
        }
    }
}
