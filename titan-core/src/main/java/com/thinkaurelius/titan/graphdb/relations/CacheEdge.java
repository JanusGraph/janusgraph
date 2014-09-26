package com.thinkaurelius.titan.graphdb.relations;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheEdge extends AbstractEdge {

    public CacheEdge(long id, EdgeLabel label, InternalVertex start, InternalVertex end, Entry data) {
        super(id, label, start.it(), end.it());
        assert data != null;

        this.data = data;
    }

    public Direction getVertexCentricDirection() {
        return data.getCache().direction;
    }

    //############## Similar code as CacheProperty but be careful when copying #############################

    private final Entry data;

    @Override
    public InternalRelation it() {
        InternalRelation it = null;
        InternalVertex startVertex = getVertex(0);

        if (startVertex.hasAddedRelations() && startVertex.hasRemovedRelations()) {
            //Test whether this relation has been replaced
            final long id = super.getLongId();
            Iterable<InternalRelation> previous = startVertex.getAddedRelations(new Predicate<InternalRelation>() {
                @Override
                public boolean apply(@Nullable InternalRelation internalRelation) {
                    return (internalRelation instanceof StandardEdge) && ((StandardEdge) internalRelation).getPreviousID() == id;
                }
            });
            assert Iterables.size(previous) <= 1 || (isLoop() && Iterables.size(previous) == 2);
            it = Iterables.getFirst(previous, null);
        }

        if (it != null)
            return it;

        return super.it();
    }

    private void copyProperties(InternalRelation to) {
        for (LongObjectCursor<Object> entry : getPropertyMap()) {
            to.setPropertyDirect(tx().getExistingRelationType(entry.key), entry.value);
        }
    }

    private synchronized InternalRelation update() {
        StandardEdge copy = new StandardEdge(super.getLongId(), getEdgeLabel(), getVertex(0), getVertex(1), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        StandardEdge u = (StandardEdge) tx().addEdge(getVertex(0), getVertex(1), getLabel());
        if (type.getConsistencyModifier()!=ConsistencyModifier.FORK) u.setId(super.getLongId());
        u.setPreviousID(super.getLongId());
        copyProperties(u);
        setId(u.getLongId());
        return u;
    }

    private RelationCache getPropertyMap() {
        RelationCache map = data.getCache();
        if (map == null || !map.hasProperties()) {
            map = RelationConstructor.readRelationCache(data, tx());
        }
        return map;
    }

    @Override
    public <O> O getPropertyDirect(RelationType type) {
        return getPropertyMap().get(type.getLongId());
    }

    @Override
    public Iterable<RelationType> getPropertyKeysDirect() {
        RelationCache map = getPropertyMap();
        List<RelationType> types = new ArrayList<RelationType>(map.numProperties());

        for (LongObjectCursor<Object> entry : map) {
            types.add(tx().getExistingRelationType(entry.key));
        }

        return types;
    }

    @Override
    public void setPropertyDirect(RelationType type, Object value) {
        update().setPropertyDirect(type, value);
    }

    @Override
    public <O> O removePropertyDirect(RelationType type) {
        return update().removePropertyDirect(type);
    }

    @Override
    public byte getLifeCycle() {
        InternalVertex startVertex = getVertex(0);
        return ((startVertex.hasRemovedRelations() || startVertex.isRemoved()) && tx().isRemovedRelation(super.getLongId()))
                ? ElementLifeCycle.Removed : ElementLifeCycle.Loaded;
    }

    @Override
    public void remove() {
        if (!tx().isRemovedRelation(super.getLongId())) {
            tx().removeRelation(this);
        }// else throw InvalidElementException.removedException(this);
    }

}
