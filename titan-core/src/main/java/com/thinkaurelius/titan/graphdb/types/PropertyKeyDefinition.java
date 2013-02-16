package com.thinkaurelius.titan.graphdb.types;

import com.tinkerpop.blueprints.Element;

import java.util.Collection;

public interface PropertyKeyDefinition extends TypeDefinition {

    public Class<?> getDataType();

    public boolean hasIndex(Class<? extends Element> clazz);

    public Iterable<IndexType> getIndexes(Class<? extends Element> clazz);

    public boolean isUnique();

}
