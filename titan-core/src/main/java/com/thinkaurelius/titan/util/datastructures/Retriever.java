package com.thinkaurelius.titan.util.datastructures;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface Retriever<I,O> {

    public O get(I input);

}
