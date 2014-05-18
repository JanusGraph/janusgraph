package com.thinkaurelius.titan.core.olap;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPResult<S extends State<S>> {

    public Iterable<S> values();

    public Iterable<Map.Entry<Long,S>> entries();

    public long size();

    public S get(long vertexid);

}
