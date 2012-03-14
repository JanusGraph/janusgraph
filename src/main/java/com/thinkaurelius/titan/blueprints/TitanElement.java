package com.thinkaurelius.titan.blueprints;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Property;
import com.tinkerpop.blueprints.pgm.Element;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**

 */
public class TitanElement<T extends Node> implements Element {

    protected final T element;

    public TitanElement(final T element) {
        Preconditions.checkNotNull(element);
        this.element=element;
    }

    @Override
    public Object getProperty(String s) {
        return element.getAttribute(s);
    }

    @Override
    public Set<String> getPropertyKeys() {
        Iterator<Property> iter = element.getPropertyIterator();
        Set<String> keys = new HashSet<String>();
        for(Property p : element.getProperties()) {
            keys.add(p.getPropertyType().getName());
        }
        return keys;
    }

    @Override
    public void setProperty(String s, Object o) {
        Iterator<Property> iter = element.getPropertyIterator(s);
        while(iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        element.createProperty(s,o);
    }

    @Override
    public Object removeProperty(String s) {
        Iterator<Property> iter = element.getPropertyIterator(s);
        Object value = null;
        while(iter.hasNext()) {
            value = iter.next().getAttribute();
            iter.remove();
        }
        return value;
    }

    @Override
    public Object getId() {
        return element.getID();
    }
    
    public T getRawElement() {
        return element;
    }
    
    public int hashCode() {
        return element.hashCode();
    }
    
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (!getClass().isInstance(other)) return false;
        else return element.equals(((TitanElement)other).element);
    }
    
    public String toString() {
        return element.toString();
    }
    
}
