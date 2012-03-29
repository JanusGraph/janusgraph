package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TaggedHolder<T extends FaunusElement> extends Holder<T> {

    protected char tag;

    static {
        WritableComparator.define(TaggedHolder.class, new Comparator());
    }

    private static Class[] CLASSES = {
            FaunusVertex.class,
            FaunusEdge.class
    };

    protected Class[] getTypes() {
        return CLASSES;

    }

    public TaggedHolder() {
        super();
    }

    public TaggedHolder(final DataInput in) throws IOException {
        this.readFields(in);
    }

    public TaggedHolder(final char tag, final T element) {
        this.set(element);
        this.tag = tag;
    }

    public char getTag() {
        return this.tag;
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
        return object.getClass().equals(TaggedHolder.class) && ((TaggedHolder) object).getTag() == this.tag && ((TaggedHolder) object).get().equals(this.get());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(TaggedHolder.class);
        }

        @Override
        public int compare(byte[] holder1, int start1, int length1, byte[] holder2, int start2, int length2) {
            // first byte is the class type
            // second and third byte are the character
            // forth byte is the element type
            // the next 8 bytes are the long id
            if (holder1[3] != holder2[3]) {
                return new Byte(holder1[3]).compareTo(holder2[3]);
            }

            final Long id1 = ByteBuffer.wrap(holder1, 4, 12).getLong();
            final Long id2 = ByteBuffer.wrap(holder2, 4, 12).getLong();

            return id1.compareTo(id2);
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof TaggedHolder && b instanceof TaggedHolder)
                return ((Long) ((TaggedHolder) a).get().getId()).compareTo((Long) ((TaggedHolder) b).get().getId());
            else
                return super.compare(a, b);
        }
    }
}
