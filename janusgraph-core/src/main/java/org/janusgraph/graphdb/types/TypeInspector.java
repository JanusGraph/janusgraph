package org.janusgraph.graphdb.types;

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;

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
