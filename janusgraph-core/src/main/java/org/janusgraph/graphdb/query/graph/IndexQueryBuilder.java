// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.util.StreamIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Implementation of {@link JanusGraphIndexQuery} for string based queries that are issued directly against the specified
 * indexing backend. It is assumed that the given string conforms to the query language of the indexing backend.
 * This class does not understand or verify the provided query. However, it will introspect the query and replace
 * any reference to `v.SOME_KEY`, `e.SOME_KEY` or `p.SOME_KEY` with the respective key reference. This replacement
 * is 'dumb' in the sense that it relies on simple string replacements to accomplish this. If the key contains special characters
 * (in particular space) then it must be encapsulated in quotation marks.
 * <p>
 * In addition to the query string, a number of parameters can be specified which will be passed verbatim to the indexing
 * backend during query execution.
 * <p>
 * This class essentially just acts as a builder, uses the {@link IndexSerializer} to execute the query, and then post-processes
 * the result set to return to the user.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexQueryBuilder extends BaseQuery implements JanusGraphIndexQuery {

    private static final Logger log = LoggerFactory.getLogger(IndexQueryBuilder.class);

    private static final String VERTEX_PREFIX = "v.";
    private static final String EDGE_PREFIX = "e.";
    private static final String PROPERTY_PREFIX = "p.";

    private final StandardJanusGraphTx tx;
    private final IndexSerializer serializer;

    /**
     * The name of the indexing backend this query is directed at
     */
    private String indexName;
    /**
     * Query string conforming to the query language supported by the indexing backend.
     */
    private String query;
    /**
     * Sorting parameters
     */
    private final List<Parameter<Order>> orders;
    /**
     * Parameters passed to the indexing backend during query execution to modify the execution behavior.
     */
    private final List<Parameter> parameters;

    /**
     * Prefix to be used to identify vertex, edge or property references and trigger key parsing and conversion.
     * In most cases this will be one of the above defined static prefixes, but in some special cases, the user may
     * define this.
     */
    private String prefix;
    /**
     * Name to use for unknown keys, i.e. key references that could not be resolved to an actual type in the database.
     */
    private final String unknownKeyName;
    /**
     * In addition to limit, this type of query supports offsets.
     */
    private int offset;

    public IndexQueryBuilder(StandardJanusGraphTx tx, IndexSerializer serializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        this.tx = tx;
        this.serializer = serializer;

        parameters = new ArrayList<>();
        orders = new ArrayList<>();
        unknownKeyName = tx.getGraph().getConfiguration().getUnknownIndexKeyName();
        this.offset=0;
    }

    //################################################
    // Inspection Methods
    //################################################

    public String getIndex() {
        return indexName;
    }

    public Parameter[] getParameters() {
        return parameters.toArray(new Parameter[0]);
    }

    public String getQuery() {
        return query;
    }

    public List<Parameter<Order>> getOrders() {
        if(orders.isEmpty()){
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(orders);
    }

    public int getOffset() {
        return offset;
    }

    public String getPrefix() {
        return prefix;
    }

    @Override
    public IndexQueryBuilder setElementIdentifier(String identifier) {
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier),"Prefix may not be a blank string");
        this.prefix=identifier;
        return this;
    }

    public String getUnknownKeyName() {
        return unknownKeyName;
    }


    //################################################
    // Builder Methods
    //################################################

    public IndexQueryBuilder setIndex(String indexName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName));
        this.indexName=indexName;
        return this;
    }

    public IndexQueryBuilder setQuery(String query) {
        Preconditions.checkArgument(StringUtils.isNotBlank(query));
        this.query=query;
        return this;
    }

    @Override
    public IndexQueryBuilder offset(int offset) {
        Preconditions.checkArgument(offset>=0,"Invalid offset provided: %s",offset);
        this.offset=offset;
        return this;
    }

    @Override
    public JanusGraphIndexQuery orderBy(String key, Order order) {
        Preconditions.checkArgument(key!=null && order!=null,"Need to specify and key and an order");
        orders.add(Parameter.of(key, order));
        return this;
    }

    @Override
    public IndexQueryBuilder limit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameter(Parameter para) {
        parameters.add(para);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameters(Iterable<Parameter> paras) {
        Iterables.addAll(parameters, paras);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameters(Parameter... paras) {
        for (Parameter para: paras) addParameter(para);
        return this;
    }

    private <E extends JanusGraphElement> Stream<Result<E>> execute(ElementCategory resultType, Class<E> resultClass) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkNotNull(query);
        if (tx.hasModifications())
            log.warn("Modifications in this transaction might not be accurately reflected in this index query: {}",query);
        return serializer.executeQuery(this, resultType, tx.getTxHandle(),tx).map(r -> (Result<E>) new ResultImpl<>(tx.getConversionFunction(resultType).apply(r.getResult()), r.getScore())).filter(r -> !r.getElement().isRemoved());
    }

    private Long executeTotals(ElementCategory resultType) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkNotNull(query);
        this.setLimit(0);
        if (tx.hasModifications())
            log.warn("Modifications in this transaction might not be accurately reflected in this index query: {}",query);
        return serializer.executeTotals(this,resultType,tx.getTxHandle(),tx);
    }

    @Deprecated
    @Override
    public Iterable<Result<JanusGraphVertex>> vertices() {
        return new StreamIterable<>(vertexStream());
    }

    @Override
    public Stream<Result<JanusGraphVertex>> vertexStream() {
        setPrefixInternal(VERTEX_PREFIX);
        return execute(ElementCategory.VERTEX, JanusGraphVertex.class);
    }

    @Deprecated
    @Override
    public Iterable<Result<JanusGraphEdge>> edges() {
        return new StreamIterable<>(edgeStream());
    }

    @Override
    public Stream<Result<JanusGraphEdge>> edgeStream() {
        setPrefixInternal(EDGE_PREFIX);
        return execute(ElementCategory.EDGE, JanusGraphEdge.class);
    }

    @Deprecated
    @Override
    public Iterable<Result<JanusGraphVertexProperty>> properties() {
        return new StreamIterable<>(propertyStream());
    }

    @Override
    public Stream<Result<JanusGraphVertexProperty>> propertyStream() {
        setPrefixInternal(PROPERTY_PREFIX);
        return execute(ElementCategory.PROPERTY, JanusGraphVertexProperty.class);
    }

    @Override
    public Long vertexTotals() {
        setPrefixInternal(VERTEX_PREFIX);
        return executeTotals(ElementCategory.VERTEX);
    }

    @Override
    public Long edgeTotals() {
        setPrefixInternal(EDGE_PREFIX);
        return executeTotals(ElementCategory.EDGE);
    }

    @Override
    public Long propertyTotals() {
        setPrefixInternal(PROPERTY_PREFIX);
        return executeTotals(ElementCategory.PROPERTY);
    }

    private void setPrefixInternal(String prefix) {
        Preconditions.checkArgument(StringUtils.isNotBlank(prefix));
        if (this.prefix==null) this.prefix=prefix;
    }

    private static class ResultImpl<V extends Element> implements Result<V> {

        private final V element;
        private final double score;

        private ResultImpl(V element, double score) {
            this.element = element;
            this.score = score;
        }

        @Override
        public V getElement() {
            return element;
        }

        @Override
        public double getScore() {
            return score;
        }
    }
}
