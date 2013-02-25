package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.types.TypeDefinition;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.StringUtils;

public abstract class SystemType extends EmptyVertex implements InternalVertex, InternalType, TypeDefinition {

    private final String name;
    private final long id;
    private final boolean[] isUnique;
    private final boolean[] isStatic;
    private final boolean isModifiable;


    SystemType(String name, long id, boolean[] isUnique, boolean[] isStatic, boolean isModifiable) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        Preconditions.checkArgument(id>0);
        Preconditions.checkArgument(isUnique!=null && isUnique.length==2);
        Preconditions.checkArgument(isStatic!=null && isStatic.length==2);
        this.name = SystemTypeManager.systemETprefix + name;
        this.id = id;
        this.isUnique = isUnique;
        this.isStatic = isStatic;
        this.isModifiable = isModifiable;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isUnique(Direction direction) {
        return isUnique[EdgeDirection.position(direction)];
    }

    @Override
    public boolean isStatic(Direction dir) {
        return isStatic[EdgeDirection.position(dir)];
    }

    @Override
    public boolean uniqueLock(Direction direction) {
        return isUnique(direction);
    }

	
	/* ---------------------------------------------------------------
     * Default System TitanRelation Type (same as SystemLabel)
	 * ---------------------------------------------------------------
	 */

    @Override
    public boolean isHidden() {
        return true;
    }

    @Override
    public boolean isModifiable() {
        return isModifiable;
    }

    @Override
    public TypeGroup getGroup() {
        return SystemTypeManager.SYSTEM_TYPE_GROUP;
    }

    @Override
    public long[] getPrimaryKey() {
        return new long[0];
    }

    @Override
    public long[] getSignature() {
        return new long[0];
    }

    @Override
    public TypeDefinition getDefinition() {
        return this;
    }

    @Override
    public long getID() {
        return id;
    }

    @Override
    public boolean hasId() {
        return true;
    }

    @Override
    public void setID(long id) {
        throw new IllegalStateException("SystemType has already been assigned an id");
    }


}
