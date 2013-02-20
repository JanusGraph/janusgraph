package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Titan;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IndexType {

    private String indexName;
    private Class<? extends Element> element;

    public IndexType() {}

    public IndexType(final String indexName, final Class<? extends Element> element) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(element==Vertex.class || element==Edge.class,"Can only index vertex or edges");
        this.indexName=indexName;
        this.element=element;
    }

    public final static IndexType of(final String indexName, final Class<? extends Element> element) {
        return new IndexType(indexName,element);
    }

    public final static IndexType of(final Class<? extends Element> element) {
        return new IndexType(Titan.Token.STANDARD_INDEX,element);
    }

    public String getIndexName() {
        return indexName;
    }

    public Class<? extends Element> getElementType() {
        return element;
    }

    public boolean isStandardIndex() {
        return indexName.equals(Titan.Token.STANDARD_INDEX);
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (!getClass().equals(other.getClass())) return false;
        IndexType oth = (IndexType)other;
        return element.equals(oth.element) && indexName.equals(oth.indexName);
    }

    @Override
    public int hashCode() {
        int hash = indexName.hashCode();
        hash *= 1711;
        if (element==Vertex.class) hash = hash << 1;
        return hash;
    }

    @Override
    public String toString() {
        return indexName+":"+element.getSimpleName();
    }

}
