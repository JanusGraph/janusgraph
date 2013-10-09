package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.StringUtils;

public abstract class SystemType extends EmptyVertex implements InternalVertex, InternalType {

    private final String name;
    private final long id;
    private final boolean[] isUnique;
    private final boolean[] isStatic;
    private final boolean isModifiable;


    SystemType(String name, long id, RelationType type, boolean[] isUnique, boolean[] isStatic, boolean isModifiable) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        Preconditions.checkArgument(isUnique != null && isUnique.length == 2);
        Preconditions.checkArgument(isStatic != null && isStatic.length == 2);
        this.name = SystemTypeManager.systemETprefix + name;
        this.id = getSystemTypeId(id, type);
        this.isUnique = isUnique;
        this.isStatic = isStatic;
        this.isModifiable = isModifiable;
    }

    static final long getSystemTypeId(long id, RelationType type) {
        Preconditions.checkArgument(id > 0);
        Preconditions.checkArgument(id <= SystemTypeManager.SYSTEM_TYPE_OFFSET, "System id [%s] is too large", id);
        Preconditions.checkArgument(type.isProper());
        switch (type) {
            case EDGE:
                return IDManager.getEdgeLabelID(id);
            case PROPERTY:
                return IDManager.getPropertyKeyID(id);
            default:
                throw new AssertionError("Illegal condition: " + type);
        }
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
    public long[] getSortKey() {
        return new long[0];
    }

    @Override
    public long[] getSignature() {
        return new long[0];
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
