package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface FaunusRelation extends TitanRelation {

    public boolean isModified();

    public FaunusRelationType getType();

    public void updateLifeCycle(ElementLifeCycle.Event event);

    public void setLifeCycle(byte lifecycle);


}
