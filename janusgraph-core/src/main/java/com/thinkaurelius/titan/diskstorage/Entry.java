package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.graphdb.relations.RelationCache;

/**
 * An entry is the primitive persistence unit used in the graph database storage backend.
 * <p/>
 * An entry consists of a column and value both of which are general {@link java.nio.ByteBuffer}s.
 * The value may be null but the column may not.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface Entry extends StaticBuffer, MetaAnnotated {

    public int getValuePosition();

    public boolean hasValue();

    public StaticBuffer getColumn();

    public<T> T getColumnAs(Factory<T> factory);

    public StaticBuffer getValue();

    public<T> T getValueAs(Factory<T> factory);

    /**
     * Returns the cached parsed representation of this Entry if it exists, else NULL
     *
     * @return
     */
    public RelationCache getCache();

    /**
     * Sets the cached parsed representation of this Entry. This method does not synchronize,
     * so a previously set representation would simply be overwritten.
     *
     * @param cache
     */
    public void setCache(RelationCache cache);
}
