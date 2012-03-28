package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T extends FaunusElement> extends GenericWritable implements WritableComparable<Holder<T>> {

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
}
