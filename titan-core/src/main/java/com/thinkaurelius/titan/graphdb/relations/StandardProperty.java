package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardProperty extends AbstractProperty implements StandardRelation, ReassignableRelation {

    public StandardProperty(long id, PropertyKey type, InternalVertex vertex, Object value, byte lifecycle) {
        super(id, type, vertex, value);
        this.lifecycle = lifecycle;
    }

    //############## SAME CODE AS StandardEdge #############################

    private static final Map<RelationType, Object> EMPTY_PROPERTIES = ImmutableMap.of();

    private byte lifecycle;
    private long previousID = 0;
    private volatile Map<RelationType, Object> properties = EMPTY_PROPERTIES;

    @Override
    public long getPreviousID() {
        return previousID;
    }

    @Override
    public void setPreviousID(long previousID) {
        Preconditions.checkArgument(previousID > 0);
        Preconditions.checkArgument(this.previousID == 0);
        this.previousID = previousID;
    }

    @Override
    public <O> O getPropertyDirect(RelationType type) {
        return (O) properties.get(type);
    }

    @Override
    public void setPropertyDirect(RelationType type, Object value) {
        Preconditions.checkArgument(!(type instanceof ImplicitKey),"Cannot use implicit type [%s] when setting property",type.getName());
        if (properties == EMPTY_PROPERTIES) {
            if (tx().getConfiguration().isSingleThreaded()) {
                properties = new HashMap<RelationType, Object>(5);
            } else {
                synchronized (this) {
                    if (properties == EMPTY_PROPERTIES) {
                        properties = Collections.synchronizedMap(new HashMap<RelationType, Object>(5));
                    }
                }
            }
        }
        properties.put(type, value);
    }

    @Override
    public Iterable<RelationType> getPropertyKeysDirect() {
        return properties.keySet();
    }

    @Override
    public <O> O removePropertyDirect(RelationType type) {
        if (!properties.isEmpty())
            return (O) properties.remove(type);
        else return null;
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }

    @Override
    public synchronized void remove() {
        if (!ElementLifeCycle.isRemoved(lifecycle)) {
            tx().removeRelation(this);
            lifecycle = ElementLifeCycle.update(lifecycle, ElementLifeCycle.Event.REMOVED);
        } //else throw InvalidElementException.removedException(this);
    }
}
