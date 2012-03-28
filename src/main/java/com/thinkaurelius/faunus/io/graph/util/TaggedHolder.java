package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TaggedHolder<T extends FaunusElement> extends Holder<T> {

    protected char tag;

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
}
