package org.janusgraph.hadoop.formats.util.input;

import org.janusgraph.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexReader {

    public long getVertexId(StaticBuffer key);

}
