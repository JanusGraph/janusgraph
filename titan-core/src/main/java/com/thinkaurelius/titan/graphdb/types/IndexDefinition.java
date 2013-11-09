package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.Titan;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Arrays;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexDefinition {

    private final String indexName;
    private final Class<? extends Element> element;
    private final Parameter[] parameters;

    public IndexDefinition(String indexName, Class<? extends Element> element, Parameter... parameters) {
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName));
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(Vertex.class.isAssignableFrom(element) || Edge.class.isAssignableFrom(element),"Element must be vertex or edge: %s",element);
        Preconditions.checkNotNull(parameters);
        this.indexName = indexName;
        this.element = element;
        this.parameters = parameters;
    }

    public final static IndexDefinition of(final String indexName, final Class<? extends Element> element, Parameter... paras) {
        return new IndexDefinition(indexName,element,paras);
    }

    public final static IndexDefinition of(final Class<? extends Element> element) {
        return new IndexDefinition(Titan.Token.STANDARD_INDEX,element);
    }

    public final static IndexDefinition of(final IndexType type, final IndexParameters paras) {
        Preconditions.checkArgument(type.getIndexName().equals(paras.getIndexName()));
        return new IndexDefinition(type.getIndexName(),type.getElementType(),paras.getParameters());
    }

    public String getIndexName() {
        return indexName;
    }

    public Class<? extends Element> getElementType() {
        return element;
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    public boolean isStandardIndex() {
        return indexName.equals(Titan.Token.STANDARD_INDEX);
    }

    public IndexType getIndexType() {
        return new IndexType(indexName,element);
    }

    public IndexParameters getIndexParamters() {
        return new IndexParameters(indexName,parameters);
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (!getClass().equals(other.getClass())) return false;
        IndexDefinition oth = (IndexDefinition)other;
        return element.equals(oth.element) && indexName.equals(oth.indexName)
                && Arrays.deepEquals(parameters,oth.parameters);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(indexName).append(element).append(parameters).toHashCode();
    }

    @Override
    public String toString() {
        return indexName+":"+element.getSimpleName()+"("+Arrays.deepToString(parameters)+")";
    }

}
