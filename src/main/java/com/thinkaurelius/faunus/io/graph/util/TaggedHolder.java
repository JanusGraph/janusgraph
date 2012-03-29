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
        public int compare(final byte[] holder1, final int start1, final int length1, final byte[] holder2, final int start2, final int length2) {
            // 0 byte is the class type
            // 1 & 2 and third byte are the character
            // 3 byte is the element type
            // the next 8 bytes are the long id

            final ByteBuffer buffer1 = ByteBuffer.wrap(holder1);
            final ByteBuffer buffer2 = ByteBuffer.wrap(holder2);

            buffer1.get();
            buffer2.get();

            buffer1.getChar();
            buffer2.getChar();

            final Byte type1 = buffer1.get();
            final Byte type2 = buffer2.get();
            if (!type1.equals(type2)) {
                return type1.compareTo(type2);
            }
            return (((Long) buffer1.getLong()).compareTo(buffer2.getLong()));
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
