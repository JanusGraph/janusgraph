package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardProperty extends AbstractProperty {

    public StandardProperty(long id, TitanKey type, InternalVertex vertex, Object value, byte lifecycle) {
        super(id, type, vertex, value);
        this.lifecycle = lifecycle;
    }

    //############## SAME CODE AS StandardEdge #############################

    private static final Map<TitanType,Object> EMPTY_PROPERTIES = ImmutableMap.of();

    private byte lifecycle;
    private long previousID=0;
    private Map<TitanType,Object> properties = EMPTY_PROPERTIES;


    public long getPreviousID() {
        return previousID;
    }

    public void setPreviousID(long previousID) {
        Preconditions.checkArgument(previousID > 0);
        Preconditions.checkArgument(this.previousID==0);
        this.previousID=previousID;
    }

    @Override
    public Object getPropertyDirect(TitanType type) {
        return properties.get(type);
    }

    @Override
    public void setPropertyDirect(TitanType type, Object value) {
        if (properties==EMPTY_PROPERTIES) {
            if (tx().getConfiguration().isSingleThreaded()) {
                properties = new HashMap<TitanType, Object>();
            } else {
                synchronized (this) {
                    if (properties==EMPTY_PROPERTIES) {
                        properties = new ConcurrentHashMap<TitanType, Object>();
                    }
                }
            }
        }
        properties.put(type,value);
    }

    @Override
    public Iterable<TitanType> getPropertyKeysDirect() {
        return properties.keySet();
    }

    @Override
    public Object removePropertyDirect(TitanType type) {
        if (!properties.isEmpty())
            return properties.remove(type);
        else return null;
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }

    @Override
    public synchronized void remove() {
        verifyRemoval();
        tx().removeRelation(this);
        lifecycle = ElementLifeCycle.update(lifecycle, ElementLifeCycle.Event.REMOVED);
    }
}
