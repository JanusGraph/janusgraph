package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheProperty extends AbstractProperty {

    public CacheProperty(long id, TitanKey key, InternalVertex start, Object value, Entry data) {
        super(id, key, start, value);
        this.data = data;
    }

    //############## Similar code as CacheEdge but be careful when copying #############################

    private final Entry data;

    @Override
    public InternalRelation it() {
        InternalRelation it = null;
        if (getVertex(0).hasAddedRelations() && getVertex(0).hasRemovedRelations()) {
            //Test whether this relation has been replaced
            final long id = super.getID();
            it = Iterables.getOnlyElement(getVertex(0).getAddedRelations(new Predicate<InternalRelation>() {
                @Override
                public boolean apply(@Nullable InternalRelation internalRelation) {
                    return (internalRelation instanceof StandardProperty) && ((StandardProperty) internalRelation).getPreviousID() == id;
                }
            }), null);
        }
        if (it != null) return it;
        else return super.it();
    }

    private final void copyProperties(InternalRelation to) {
        ImmutableLongObjectMap map = getMap();
        for (int i = 0; i < map.size(); i++) {
            if (map.getKey(i) < 0) continue;
            TitanType type = tx().getExistingType(map.getKey(i));
            to.setPropertyDirect(type, map.getValue(i));
        }
    }

    private synchronized InternalRelation update() {
        StandardProperty copy = new StandardProperty(super.getID(), getPropertyKey(), getVertex(0), getValue(), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        StandardProperty u = (StandardProperty) tx().addProperty(getVertex(0), getPropertyKey(), getValue());
        u.setPreviousID(super.getID());
        copyProperties(u);
        return u;
    }

    @Override
    public long getID() {
        InternalRelation it = it();
        if (it == this) return super.getID();
        else return it.getID();
    }

    private ImmutableLongObjectMap getMap() {
        ImmutableLongObjectMap map = data.getCache();
        if (map == null) {
            map = tx().getGraph().getEdgeSerializer().readProperties(getVertex(0), data, tx());
        }
        return map;
    }

    @Override
    public <O> O getPropertyDirect(TitanType type) {
        return getMap().get(type.getID());
    }

    @Override
    public Iterable<TitanType> getPropertyKeysDirect() {
        ImmutableLongObjectMap map = getMap();
        List<TitanType> types = new ArrayList<TitanType>(map.size());
        for (int i = 0; i < map.size(); i++) {
            if (map.getKey(i) < 0) continue;
            if (map.getValue(i) != null)
                types.add(tx().getExistingType(map.getKey(i)));
        }
        return types;
    }

    @Override
    public void setPropertyDirect(TitanType type, Object value) {
        update().setPropertyDirect(type, value);
    }

    @Override
    public <O> O removePropertyDirect(TitanType type) {
        return update().removePropertyDirect(type);
    }

    @Override
    public byte getLifeCycle() {
        if ((getVertex(0).hasRemovedRelations() || getVertex(0).isRemoved()) && tx().isRemovedRelation(super.getID()))
            return ElementLifeCycle.Removed;
        else return ElementLifeCycle.Loaded;
    }

    @Override
    public void remove() {
        verifyRemoval();
        if (!tx().isRemovedRelation(super.getID())) {
            tx().removeRelation(this);
        }
    }


}
