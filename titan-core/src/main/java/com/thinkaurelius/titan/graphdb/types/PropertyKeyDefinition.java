package com.thinkaurelius.titan.graphdb.types;

import com.tinkerpop.blueprints.Element;

public interface PropertyKeyDefinition extends TypeDefinition {

    public Class<?> getDataType();

    public Iterable<String> getIndexes(Class<? extends Element> clazz);

    public boolean hasIndex(String name, Class<? extends Element> elementType);

}
