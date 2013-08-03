package com.thinkaurelius.titan.graphdb.types;

public class StandardLabelDefinition extends AbstractTypeDefinition implements EdgeLabelDefinition {

    private boolean isUnidirectional;

    StandardLabelDefinition() {
    }

    public StandardLabelDefinition(String name,
                                   boolean[] unique, boolean[] hasUniqueLock, boolean[] isStatic,
                                   boolean hidden, boolean modifiable,
                                   long[] primaryKey, long[] signature, boolean unidirectional) {
        super(name, unique, hasUniqueLock, isStatic, hidden, modifiable, primaryKey, signature);
        isUnidirectional = unidirectional;
    }

    @Override
    public boolean isUnidirectional() {
        return isUnidirectional;
    }
}
