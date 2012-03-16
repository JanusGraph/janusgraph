package com.thinkaurelius.titan.blueprints;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Property;
import com.thinkaurelius.titan.core.PropertyType;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**

 */
public class TitanElement<T extends Node> implements Element {

    protected final T element;
    protected final TitanGraph db;

    public TitanElement(final T element, final TitanGraph db) {
        Preconditions.checkNotNull(element);
        Preconditions.checkNotNull(db);
        this.element=element;
        this.db=db;
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
        if (s.equals(StringFactory.ID) || (s.equals(StringFactory.LABEL) && this instanceof Edge))
            throw new IllegalArgumentException(s + StringFactory.PROPERTY_EXCEPTION_MESSAGE);

        PropertyType pt = db.getPropertyType(s);
        Iterator<Property> iter = element.getPropertyIterator(pt);
        while(iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        element.createProperty(pt,o);
        db.operation();
    }

    @Override
    public Object removeProperty(String s) {
        Iterator<Property> iter = element.getPropertyIterator(s);
        Object value = null;
        while(iter.hasNext()) {
            value = iter.next().getAttribute();
            iter.remove();
        }
        db.operation();
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
