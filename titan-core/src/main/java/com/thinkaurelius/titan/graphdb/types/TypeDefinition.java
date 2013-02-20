package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.tinkerpop.blueprints.Direction;


public interface TypeDefinition {

    /**
     * Returns the name of this association type. Names must be unique across all association types.
     *
     * @return Name of this association.
     */
    public String getName();

    public TypeGroup getGroup();

    public long[] getPrimaryKey();

    public long[] getSignature();

    public boolean uniqueLock(Direction direction);

    public boolean isUnique(Direction direction);

    public boolean isStatic(Direction dir);

    /**
     * Checks whether this type is hidden.
     * If a type is hidden, its relations are not included in edge retrieval operations. Types used internally
     * are hidden so they don't interfere with user types.
     *
     * @return true, if the type is hidden, else false.
     * @see com.thinkaurelius.titan.graphdb.types.system.SystemType
     */
    public boolean isHidden();

    public boolean isModifiable();

}
