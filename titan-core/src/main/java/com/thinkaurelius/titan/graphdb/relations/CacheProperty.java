package com.thinkaurelius.titan.graphdb.relations;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheProperty extends AbstractProperty {

    public CacheProperty(long id, PropertyKey key, InternalVertex start, Object value, Entry data) {
        super(id, key, start.it(), value);
        this.data = data;
    }

    //############## Similar code as CacheEdge but be careful when copying #############################

    private final Entry data;

    @Override
    public InternalRelation it() {
        InternalRelation it = null;
        InternalVertex startVertex = getVertex(0);

        if (startVertex.hasAddedRelations() && startVertex.hasRemovedRelations()) {
            //Test whether this relation has been replaced
            final long id = super.getLongId();
            it = Iterables.getOnlyElement(startVertex.getAddedRelations(new Predicate<InternalRelation>() {
                @Override
                public boolean apply(@Nullable InternalRelation internalRelation) {
                    return (internalRelation instanceof StandardProperty) && ((StandardProperty) internalRelation).getPreviousID() == id;
                }
            }), null);
        }

        return (it != null) ? it : super.it();
    }

    private void copyProperties(InternalRelation to) {
        for (LongObjectCursor<Object> entry : getPropertyMap()) {
            to.setPropertyDirect(tx().getExistingRelationType(entry.key), entry.value);
        }
    }

    private synchronized InternalRelation update() {
        StandardProperty copy = new StandardProperty(super.getLongId(), getPropertyKey(), getVertex(0), getValue(), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        StandardProperty u = (StandardProperty) tx().addProperty(getVertex(0), getPropertyKey(), getValue());
        if (type.getConsistencyModifier()!= ConsistencyModifier.FORK) u.setId(super.getLongId());
        u.setPreviousID(super.getLongId());
        copyProperties(u);
        return u;
    }

    @Override
    public long getLongId() {
        InternalRelation it = it();
        return (it == this) ? super.getLongId() : it.getLongId();
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
        if ((getVertex(0).hasRemovedRelations() || getVertex(0).isRemoved()) && tx().isRemovedRelation(super.getLongId()))
            return ElementLifeCycle.Removed;
        else return ElementLifeCycle.Loaded;
    }

    @Override
    public void remove() {
        if (!tx().isRemovedRelation(super.getLongId())) {
            tx().removeRelation(this);
        }// else throw InvalidElementException.removedException(this);
    }


}
