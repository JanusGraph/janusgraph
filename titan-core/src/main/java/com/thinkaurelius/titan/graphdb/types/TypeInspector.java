package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeInspector {

    public RelationType getExistingRelationType(long id);

    public VertexLabel getExistingVertexLabel(long id);

    public boolean containsRelationType(String name);

    public RelationType getRelationType(String name);

}
