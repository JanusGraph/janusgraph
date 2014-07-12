package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LifeCycleElement {

    protected byte lifecycle = ElementLifeCycle.New;

    public void updateLifeCycle(ElementLifeCycle.Event event) {
        this.lifecycle = ElementLifeCycle.update(lifecycle,event);
    }

    public void setLifeCycle(byte lifecycle) {
        Preconditions.checkArgument(ElementLifeCycle.isValid(lifecycle));
        this.lifecycle = lifecycle;
    }

    public byte getLifeCycle() {
        return lifecycle;
    }

    public boolean isNew() {
        return ElementLifeCycle.isNew(lifecycle);
    }

    public boolean isRemoved() {
        return ElementLifeCycle.isRemoved(lifecycle);
    }

    public boolean isLoaded() {
        return ElementLifeCycle.isLoaded(lifecycle);
    }

    public boolean isModified() {
        return ElementLifeCycle.isModified(lifecycle);
    }

}
