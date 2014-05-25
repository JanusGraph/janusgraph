package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.RelationType;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeInspector {

    public RelationType getExistingRelationType(long id);

    public boolean containsRelationType(String name);

    public RelationType getRelationType(String name);

}
