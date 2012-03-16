package com.thinkaurelius.titan.blueprints;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.blueprints.TitanVertex;
import com.thinkaurelius.titan.blueprints.util.TitanVertexSequence;
import com.thinkaurelius.titan.core.PropertyType;
import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;

import java.util.Set;

public class TitanIndex<T extends Element> implements AutomaticIndex<T> {

    private final String indexName;
    private final TitanGraph graph;
    private final Set<String> keys;
    private final Class<T> indexClass;

    TitanIndex(final TitanGraph graph, final String indexName, final Set<String> autoIndexKeys, final Class<T> indexClass) {
        this.graph = graph;
        this.indexName = indexName;
        ImmutableSet.Builder<String> b = ImmutableSet.builder();
        b.addAll(autoIndexKeys);
        this.keys = b.build();
        this.indexClass = indexClass;
    }

    @Override
    public Set<String> getAutoIndexKeys() {
        return keys;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public CloseableSequence<T> get(String s, Object o) {
        if (!keys.contains(s)) return new TitanVertexSequence(db);
        return new TitanVertexSequence(db,db.indexRetrieval(s,o));
    }

    @Override
    public long count(final String key, final Object value) {
        return graph.indexRetrieval(key, value).size();
    }

    @Override
    public Class<T> getIndexClass() {
        return indexClass;
    }

    @Override
    public Type getIndexType() {
        return Type.AUTOMATIC;
    }

    @Override
    public void put(final String key, final Object value, final T element) {
        throw new UnsupportedOperationException("Cannot manually update an automatic index");
    }

    @Override
    public void remove(final String key, final Object value, final T element) {
        throw new UnsupportedOperationException("Cannot manually update an automatic index");
    }
}
