package org.janusgraph.graphdb.relations;

import org.janusgraph.graphdb.internal.InternalRelation;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface StandardRelation extends InternalRelation {

    public long getPreviousID();

    public void setPreviousID(long previousID);

}
