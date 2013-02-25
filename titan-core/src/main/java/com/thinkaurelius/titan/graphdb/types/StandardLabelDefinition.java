package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.TypeGroup;

public class StandardLabelDefinition extends AbstractTypeDefinition implements EdgeLabelDefinition {

    private boolean isUnidirectional;

    StandardLabelDefinition() {
    }

    public StandardLabelDefinition(String name, TypeGroup group,
                                   boolean[] unique, boolean[] hasUniqueLock, boolean[] isStatic,
                                   boolean hidden, boolean modifiable,
                                   long[] primaryKey, long[] signature, boolean unidirectional) {
        super(name, group, unique, hasUniqueLock, isStatic, hidden, modifiable, primaryKey, signature);
        isUnidirectional = unidirectional;
    }

    @Override
    public boolean isUnidirectional() {
        return isUnidirectional;
    }
}
