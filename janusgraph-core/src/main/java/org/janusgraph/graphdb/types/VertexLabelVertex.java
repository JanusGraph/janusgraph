package org.janusgraph.graphdb.types;

import org.janusgraph.graphdb.internal.InternalVertexLabel;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
import org.janusgraph.graphdb.types.vertices.JanusSchemaVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexLabelVertex extends JanusSchemaVertex implements InternalVertexLabel {


    public VertexLabelVertex(StandardJanusTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public boolean isPartitioned() {
        return getDefinition().getValue(TypeDefinitionCategory.PARTITIONED, Boolean.class);
    }

    @Override
    public boolean isStatic() {
        return getDefinition().getValue(TypeDefinitionCategory.STATIC, Boolean.class);
    }

    @Override
    public boolean hasDefaultConfiguration() {
        return !isPartitioned() && !isStatic();
    }

    private Integer ttl = null;

    @Override
    public int getTTL() {
        if (null == ttl) {
            ttl = TypeUtil.getTTL(this);
        }
        return ttl;
    }

}
