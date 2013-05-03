package com.thinkaurelius.titan.graphdb.query;

import cern.colt.Arrays;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAnd;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.RelationComparator;
import com.tinkerpop.blueprints.Direction;

import java.util.Comparator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexCentricQuery implements Query<VertexCentricQuery> {

    private final InternalVertex vertex;

    private final Direction dir;
    private final TitanType[] types;
    private final TypeGroup group;

    private final KeyAnd<TitanType> constraints;
    private final boolean includeHidden;
    private final int limit;
    private final RelationType returnType;

    public VertexCentricQuery(InternalVertex vertex, Direction dir, TitanType[] types, TypeGroup group,
                              KeyAnd<TitanType> constraints,
                              boolean includeHidden, int limit, RelationType returnType) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkNotNull(dir);
        Preconditions.checkNotNull(types);
        Preconditions.checkNotNull(constraints);
        Preconditions.checkNotNull(returnType);
        this.vertex = vertex;
        this.dir = dir;
        this.types = types;
        this.group = group;
        this.constraints = constraints;
        this.includeHidden = includeHidden;
        this.limit = limit;
        this.returnType=returnType;
    }

    public VertexCentricQuery(VertexCentricQuery other, int newLimit) {
        this.vertex=other.vertex;
        this.dir = other.dir;
        this.types = other.types;
        this.group = other.group;
        this.constraints = other.constraints;
        this.includeHidden = other.includeHidden;
        this.limit = newLimit;
        this.returnType=other.returnType;
    }

    private VertexCentricQuery() {
        this.vertex=null;
        this.dir=Direction.BOTH;
        this.types=new TitanType[0];
        this.group=null;
        constraints= KeyAnd.of();
        this.includeHidden=true;
        this.limit=0;
        this.returnType= RelationType.RELATION;
    }

    public InternalVertex getVertex() {
        return vertex;
    }

    public boolean hasSingleDirection() {
        return dir!=Direction.BOTH;
    }

    public Direction getDirection() {
        return dir;
    }

    public boolean hasType() {
        return types.length>0;
    }

    public int numberTypes() {
        return types.length;
    }

    public TitanType[] getTypes() {
        return types;
    }

    public boolean hasGroup() {
        return group!=null;
    }

    public TypeGroup getGroup() {
        return group;
    }

    public KeyAnd<TitanType> getConstraints() {
        return constraints;
    }

    public Multimap<TitanType,KeyAtom<TitanType>> getConstraintMap() {
        Multimap<TitanType,KeyAtom<TitanType>> constraintMap = HashMultimap.create();
        for (KeyCondition<TitanType> atom : constraints.getChildren()) {
            constraintMap.put(((KeyAtom<TitanType>)atom).getKey(),(KeyAtom)atom);
        }
        return constraintMap;
    }

    public boolean hasConstraints() {
        return constraints.hasChildren();
    }

    public boolean isIncludeHidden() {
        return includeHidden;
    }

    public RelationType getReturnType() {
        return returnType;
    }

    @Override
    public boolean hasLimit() {
        return limit!=Query.NO_LIMIT;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public Comparator getSortOrder() {
        return new RelationComparator(vertex);
    }

    @Override
    public boolean hasUniqueResults() {
        return false;
    }

    @Override
    public boolean isInvalid() {
        return limit<=0;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("[").append(vertex).append("]");
        s.append("(").append(dir).append(",").append(group).append(",").append(Arrays.toString(types)).append(")");
        s.append("-").append(constraints).append(":");
        s.append(includeHidden).append(":");
        if (hasLimit()) s.append(limit).append(":");
        s.append(returnType);
        return s.toString();
    }


    public boolean matches(TitanRelation relation) {
        InternalRelation r = (InternalRelation)relation;
        Preconditions.checkArgument(relation.isIncidentOn(vertex));
        if ((r.isProperty() && returnType== RelationType.EDGE) ||
                (r.isEdge() && returnType== RelationType.PROPERTY)) return false;
        if (!includeHidden && r.isHidden()) return false;
        if (dir!=Direction.BOTH) {
            //Check matching direction
            int pos = EdgeDirection.position(dir);
            if (pos>r.getLen() || !r.getVertex(pos).equals(vertex)) return false;
        }
        if (group!=null && !r.getType().getGroup().equals(group)) return false;
        if (types.length>0) {
            boolean matches = false;
            for (TitanType type : types) if (r.getType().equals(type)) { matches = true; break; }
            if (!matches) return false;
        }
        //Check constraints
        if (hasConstraints()) {
            if (!StandardElementQuery.matchesCondition(relation, constraints)) return false;
        }
        return true;
    }

    public static final VertexCentricQuery INVALID = new VertexCentricQuery();

}
