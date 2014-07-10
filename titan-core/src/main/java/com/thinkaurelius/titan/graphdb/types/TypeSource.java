package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeSource {

    public boolean containsRelationType(String name);

    public RelationType getRelationType(String name);

    public PropertyKey getPropertyKey(String name);

    public EdgeLabel getEdgeLabel(String name);

}
