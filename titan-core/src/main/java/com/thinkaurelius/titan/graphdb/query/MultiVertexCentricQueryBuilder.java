package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.DirectionCondition;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MultiVertexCentricQueryBuilder extends AbstractVertexCentricQueryBuilder implements TitanMultiVertexQuery {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(MultiVertexCentricQueryBuilder.class);

    private final Set<InternalVertex> vertices;

    public MultiVertexCentricQueryBuilder(final StandardTitanTx tx, final EdgeSerializer serializer) {
        super(tx, serializer);
        vertices = Sets.newHashSet();
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    @Override
    public TitanMultiVertexQuery addVertex(TitanVertex vertex) {
        assert vertex != null;
        assert vertex instanceof InternalVertex;
        vertices.add((InternalVertex)vertex);
        return this;
    }

    @Override
    public TitanMultiVertexQuery addAllVertices(Collection<TitanVertex> vertices) {
        for (TitanVertex v : vertices) addVertex(v);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder has(TitanKey key, Object value) {
        super.has(key, value);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder has(TitanLabel label, TitanVertex vertex) {
        super.has(label, vertex);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder has(String type, Object value) {
        super.has(type, value);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder hasNot(String key, Object value) {
        super.hasNot(key, value);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder has(String key, Predicate predicate, Object value) {
        super.has(key, predicate, value);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder has(TitanKey key, Predicate predicate, Object value) {
        super.has(key, predicate, value);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder has(String key) {
        super.has(key);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder hasNot(String key) {
        super.hasNot(key);
        return this;
    }

    @Override
    public <T extends Comparable<?>> MultiVertexCentricQueryBuilder interval(TitanKey key, T start, T end) {
        super.interval(key, start, end);
        return this;
    }

    @Override
    public <T extends Comparable<?>> MultiVertexCentricQueryBuilder interval(String key, T start, T end) {
        super.interval(key, start, end);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder types(TitanType... types) {
        super.types(types);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder labels(String... labels) {
        super.labels(labels);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder keys(String... keys) {
        super.keys(keys);
        return this;
    }

    public MultiVertexCentricQueryBuilder type(TitanType type) {
        super.type(type);
        return this;
    }

    public MultiVertexCentricQueryBuilder type(String type) {
        super.type(type);
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder direction(Direction d) {
        super.direction(d);
        return this;
    }

    public MultiVertexCentricQueryBuilder includeHidden() {
        super.includeHidden();
        return this;
    }

    @Override
    public MultiVertexCentricQueryBuilder limit(int limit) {
        super.limit(limit);
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    @Override
    protected EdgeSerializer.VertexConstraint getVertexConstraint() {
        return null;
    }


    protected Map<TitanVertex, Iterable<? extends TitanRelation>> relations(RelationType returnType) {
        Preconditions.checkArgument(!vertices.isEmpty(), "Need to add at least one vertex to query");
        BaseVertexCentricQuery vq = super.constructQuery(returnType);
        Map<TitanVertex, Iterable<? extends TitanRelation>> result = new HashMap<TitanVertex, Iterable<? extends TitanRelation>>(vertices.size());
        if (!vq.isEmpty()) {
            for (BackendQueryHolder<SliceQuery> sq : vq.getQueries()) {
                tx.executeMultiQuery(vertices, sq.getBackendQuery());
            }

            Condition<TitanRelation> condition = vq.getCondition();
            for (InternalVertex v : vertices) {
                //Add other-vertex and direction related conditions
                And<TitanRelation> newcond = new And<TitanRelation>();
                if (condition instanceof And) newcond.addAll((And) condition);
                else newcond.add(condition);
                newcond.add(new DirectionCondition<TitanRelation>(v, getDirection()));
                VertexCentricQuery vqsingle = new VertexCentricQuery(v, newcond, vq.getDirection(), vq.getQueries(), vq.getLimit());
                result.put(v, new QueryProcessor<VertexCentricQuery, TitanRelation, SliceQuery>(vqsingle, tx.edgeProcessor));

            }
        } else {
            Iterable<? extends TitanRelation> emptyIter = IterablesUtil.emptyIterable();
            for (TitanVertex v : vertices)
                result.put(v, emptyIter);
        }
        return result;
    }


    @Override
    public Map<TitanVertex, Iterable<TitanEdge>> titanEdges() {
        return (Map) relations(RelationType.EDGE);
    }

    @Override
    public Map<TitanVertex, Iterable<TitanProperty>> properties() {
        return (Map) relations(RelationType.PROPERTY);
    }

    @Override
    public Map<TitanVertex, Iterable<TitanRelation>> relations() {
        return (Map) relations(RelationType.RELATION);
    }

    @Override
    public Map<TitanVertex, Iterable<TitanVertex>> vertices() {
        Map<TitanVertex, Iterable<TitanEdge>> base = titanEdges();
        Map<TitanVertex, Iterable<TitanVertex>> result = new HashMap<TitanVertex, Iterable<TitanVertex>>(base.size());
        for (Map.Entry<TitanVertex, Iterable<TitanEdge>> entry : base.entrySet()) {
            result.put(entry.getKey(), edges2Vertices(entry.getValue(), entry.getKey()));
        }
        return result;
    }

    @Override
    public Map<TitanVertex, VertexList> vertexIds() {
        Map<TitanVertex, Iterable<TitanEdge>> base = titanEdges();
        Map<TitanVertex, VertexList> result = new HashMap<TitanVertex, VertexList>(base.size());
        for (Map.Entry<TitanVertex, Iterable<TitanEdge>> entry : base.entrySet()) {
            result.put(entry.getKey(), edges2VertexIds(entry.getValue(), entry.getKey()));
        }
        return result;
    }

}
