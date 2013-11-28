package com.thinkaurelius.titan.graphdb.types.reference;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.thinkaurelius.titan.graphdb.types.TypeAttributeType;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanLabelReference extends TitanTypeReference implements TitanLabel {

    public TitanLabelReference(TitanLabel type) {
        super((TitanTypeVertex)type);
    }

    public TitanLabelReference(long id, String name, TypeAttribute.Map definition) {
        super(id, name, definition);
    }

    //######## IDENTICAL TO TitanLabelVertex

    public boolean isDirected() {
        return !isUnidirected();
    }

    public boolean isUnidirected() {
        return getDefinition().getValue(TypeAttributeType.UNIDIRECTIONAL,boolean.class);
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }



}
