package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.graphdb.internal.InternalRelation;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface StandardRelation extends InternalRelation {

    public long getPreviousID();

    public void setPreviousID(long previousID);

}
