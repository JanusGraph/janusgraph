package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanManagement extends TitanConfiguration {

    public TitanTypeIndex createTypeIndex(TitanLabel label, String name, Direction direction, TitanType... sortKeys);

    public TitanTypeIndex createTypeIndex(TitanLabel label, String name, Direction direction, Order sortOrder, TitanType... sortKeys);

    public TitanTypeIndex createTypeIndex(TitanKey key, String name, TitanType... sortKeys);

    public TitanTypeIndex createTypeIndex(TitanKey key, String name, Order sortOrder, TitanType... sortKeys);

    public boolean containsTypeIndex(TitanType type, String name);

    public TitanTypeIndex getTypeIndex(TitanType type, String name);

    public Iterable<TitanTypeIndex> getTypeIndexes(TitanType type);


    public boolean containsGraphIndex(String name);

    public TitanGraphIndex getGraphIndex(String name);

    public Iterable<TitanGraphIndex> getGraphIndexes(final Class<? extends Element> elementType);

    public TitanGraphIndex createExternalIndex(String indexName, Class<? extends Element> elementType, String backingIndex);

    public void addIndexKey(final TitanGraphIndex index, final TitanKey key, Parameter... parameters);

    public TitanGraphIndex createInternalIndex(String indexName, Class<? extends Element> elementType, TitanKey... keys);

    public TitanGraphIndex createInternalIndex(String indexName, Class<? extends Element> elementType, boolean unique, TitanKey... keys);


    public ConsistencyModifier getConsistency(TitanSchemaElement element);

    public void setConsistency(TitanSchemaElement element, ConsistencyModifier consistency);



    public boolean containsType(String name);

    public TitanType getType(String name);

    public TitanKey getPropertyKey(String name);

    public TitanLabel getEdgeLabel(String name);

    public KeyMaker makeKey(String name);

    public LabelMaker makeLabel(String name);

    /**
     * Returns an iterable over all defined types that have the given clazz (either {@link TitanLabel} which returns all labels,
     * {@link TitanKey} which returns all keys, or {@link TitanType} which returns all types).
     *
     * @param clazz {@link TitanType} or sub-interface
     * @param <T>
     * @return Iterable over all types for the given category (label, key, or both)
     */
    public <T extends TitanType> Iterable<T> getTypes(Class<T> clazz);


    public void commit();

    public void rollback();

}
