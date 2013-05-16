package com.thinkaurelius.titan.graphdb.relations;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface StandardRelation {

    public long getPreviousID();

    public void setPreviousID(long previousID);

}
