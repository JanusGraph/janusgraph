package org.janusgraph.graphdb.database;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface RelationReader {

    public RelationCache parseRelation(Entry data, boolean parseHeaderOnly, TypeInspector tx);

}
