package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeInspector {

    public default PropertyKey getExistingPropertyKey(long id) {
        return (PropertyKey)getExistingRelationType(id);
    }

    public default EdgeLabel getExistingEdgeLabel(long id) {
        return (EdgeLabel)getExistingRelationType(id);
    }

    public RelationType getExistingRelationType(long id);

    public VertexLabel getExistingVertexLabel(long id);

    public boolean containsRelationType(String name);

    public RelationType getRelationType(String name);

}
