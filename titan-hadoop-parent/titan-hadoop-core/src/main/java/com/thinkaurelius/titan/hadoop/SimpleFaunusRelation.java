package com.thinkaurelius.titan.hadoop;

import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class SimpleFaunusRelation extends LifeCycleElement implements FaunusRelation {

    protected abstract Object otherValue();

    @Override
    public boolean isProperty() {
        return getType().isPropertyKey();
    }

    @Override
    public boolean isEdge() {
        return getType().isEdgeLabel();
    }

    //##################################
    // General Utility
    //##################################


    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(otherValue()).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (oth == null || !getClass().isInstance(oth)) return false;
        SimpleFaunusProperty p = (SimpleFaunusProperty) oth;
        return getType().equals(p.getType()) && otherValue().equals(p.otherValue());
    }

    @Override
    public String toString() {
        return getType().getName() + "->" + otherValue();
    }

    //##################################
    // Default
    //##################################

    @Override
    public Object getId() {
        return getLongId();
    }

    @Override
    public long getLongId() {
        return FaunusElement.NO_ID;
    }

    @Override
    public boolean hasId() {
        return false;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(PropertyKey key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <O> O getProperty(PropertyKey key) {
        return null;
    }

    @Override
    public <O> O getProperty(String key) {
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return Sets.newHashSet();
    }

    @Override
    public <O> O removeProperty(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <O> O removeProperty(RelationType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(EdgeLabel label, TitanVertex vertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TitanVertex getProperty(EdgeLabel label) {
        return null;
    }

    @Override
    public Direction getDirection(TitanVertex vertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isIncidentOn(TitanVertex vertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLoop() {
        return false;
    }

}
