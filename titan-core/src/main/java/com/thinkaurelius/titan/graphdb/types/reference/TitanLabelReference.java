package com.thinkaurelius.titan.graphdb.types.reference;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanLabelReference extends TitanTypeReference implements TitanLabel {

    public TitanLabelReference(TitanLabel type) {
        super((TitanTypeVertex)type);
    }

    public TitanLabelReference(long id, String name, TypeDefinitionMap definition) {
        super(id, name, definition);
    }

    //######## IDENTICAL TO TitanLabelVertex

    public boolean isDirected() {
        return !isUnidirected();
    }

    public boolean isUnidirected() {
        return getDefinition().getValue(TypeDefinitionCategory.UNIDIRECTIONAL,boolean.class);
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
