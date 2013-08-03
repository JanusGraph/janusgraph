package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAnd;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.TitanPredicate;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.tinkerpop.blueprints.*;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class VertexCentricQueryBuilder implements TitanVertexQuery {

    private static final Logger log = LoggerFactory.getLogger(VertexCentricQueryBuilder.class);
    
    private final InternalVertex vertex;

    private Direction dir;
    private Set<String> types;
    private List<KeyAtom<String>> constraints;
    private boolean includeHidden;
    private int limit = Query.NO_LIMIT;


    public VertexCentricQueryBuilder(InternalVertex v) {
        Preconditions.checkNotNull(v);
        this.vertex=v;

        dir = Direction.BOTH;
        types = new HashSet<String>(4);
        constraints = Lists.newArrayList();
        includeHidden = false;
    }

    private final TitanType getType(String typeName) {
        TitanType t = vertex.tx().getType(typeName);
        if (t == null && !vertex.tx().getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return t;
    }

	/* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    private VertexCentricQuery constructQuery(RelationType returnType) {
        Preconditions.checkNotNull(returnType);
        Preconditions.checkArgument(limit>=0);
        Preconditions.checkArgument(dir!=null);

        if (limit==0) return VertexCentricQuery.INVALID;

        if (returnType== RelationType.PROPERTY) {
            if (dir==Direction.IN) return VertexCentricQuery.INVALID;
            else dir=Direction.OUT;
        }

        List<TitanType> ts = Lists.newArrayList();
        if (!types.isEmpty()) {
            for (String type : types) {
                TitanType t = getType(type);
                if (t!=null) {
                    ts.add(t);
                    if (t.isPropertyKey()) {
                        if (returnType==RelationType.EDGE)
                            throw new IllegalArgumentException("Querying for edges but including a property key: " + t.getName());
                        returnType=RelationType.PROPERTY;
                    }
                    if (t.isEdgeLabel()) {
                        if (returnType== RelationType.PROPERTY)
                            throw new IllegalArgumentException("Querying for properties but including an edge label: " + t.getName());
                        returnType = RelationType.EDGE;
                    }
                }
            }
            if (ts.isEmpty()) return VertexCentricQuery.INVALID;
        }

        //check constraints
        List<KeyAtom<TitanType>> c = new ArrayList<KeyAtom<TitanType>>(constraints.size());
        for (int i=0;i<constraints.size();i++) {
            KeyAtom<String> atom = constraints.get(i);
            TitanType t = getType(atom.getKey());
            if (t==null) {
                if (atom.getTitanPredicate()==Cmp.EQUAL && atom.getCondition()==null) continue; //Ignore condition
                else return VertexCentricQuery.INVALID;
            }
            Object condition = atom.getCondition();
            TitanPredicate titanPredicate = atom.getTitanPredicate();
            //Check condition
            Preconditions.checkArgument(titanPredicate.isValidCondition(condition),"Invalid condition onf key [%s]: %s",t.getName(),condition);
            if (t.isPropertyKey()) {
                condition = AttributeUtil.verifyAttributeQuery((TitanKey)t,condition);
                Preconditions.checkArgument(titanPredicate.isValidCondition(condition),"Invalid condition: %s",condition);
//                Preconditions.checkArgument(relation.isValidDataType(((TitanKey)t).getDataType()),"Invalid data type for condition");
            } else { //t.isEdgeLabel()
                Preconditions.checkArgument(((TitanLabel)t).isUnidirected() && (condition instanceof TitanVertex));
            }
            c.add(new KeyAtom<TitanType>(t, titanPredicate, condition));
        }

        return new VertexCentricQuery(vertex,dir,ts.toArray(new TitanType[ts.size()]),KeyAnd.of(c.toArray(new KeyAtom[c.size()])),includeHidden,limit,returnType);
    }


    @Override
    public Iterable<Edge> edges() {
        return (Iterable)titanEdges();
    }

    @Override
    public Iterable<Vertex> vertices() {
        return (Iterable)Iterables.transform(titanEdges(),new Function<TitanEdge, TitanVertex>() {
            @Nullable
            @Override
            public TitanVertex apply(@Nullable TitanEdge titanEdge) {
                return titanEdge.getOtherVertex(vertex);
            }
        });
    }

    public Iterable<TitanRelation> relations(RelationType returnType) {
        VertexCentricQuery query = constructQuery(returnType);
        QueryProcessor<VertexCentricQuery,TitanRelation> processor =
                new QueryProcessor<VertexCentricQuery, TitanRelation>(query,vertex.tx().edgeProcessor,VertexCentricQueryOptimizer.INSTANCE);
        return processor;
    }


    @Override
    public Iterable<TitanEdge> titanEdges() {
        return Iterables.filter(relations(RelationType.EDGE),TitanEdge.class);
    }

    @Override
    public Iterable<TitanProperty> properties() {
        return Iterables.filter(relations(RelationType.PROPERTY),TitanProperty.class);
    }

    @Override
    public Iterable<TitanRelation> relations() {
        return relations(RelationType.RELATION);
    }

    @Override
    public long count() {
        return Iterables.size(edges());
    }

    @Override
    public long propertyCount() {
        return Iterables.size(properties());
    }

    @Override
    public VertexList vertexIds() {
        VertexArrayList vertices = new VertexArrayList();
        for (TitanEdge edge : titanEdges()) vertices.add(edge.getOtherVertex(vertex));
        return vertices;
    }


    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private VertexCentricQueryBuilder addConstraint(String type, TitanPredicate rel, Object value) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(rel);
        constraints.add(new KeyAtom<String>(type, rel, value));
        return this;
    }

    @Override
    public VertexCentricQueryBuilder has(TitanKey key, Object value) {
        return has(key.getName(),value);
    }

    @Override
    public VertexCentricQueryBuilder has(TitanLabel label, TitanVertex vertex) {
        return has(label.getName(), vertex);
    }

    @Override
    public VertexCentricQueryBuilder has(String type, Object value) {
        return addConstraint(type,Cmp.EQUAL,value);
    }

    @Override
    public TitanVertexQuery hasNot(String key, Object value) {
        return has(key,Cmp.NOT_EQUAL,value);
    }

    @Override
    public TitanVertexQuery has(String key, Predicate predicate, Object value) {
        return addConstraint(key,TitanPredicate.Converter.convert(predicate),value);
    }

    @Override
    public TitanVertexQuery has(TitanKey key, Predicate predicate, Object value) {
        return has(key.getName(),predicate,value);
    }

    @Override
    public TitanVertexQuery has(String key) {
        return has(key,Cmp.NOT_EQUAL,(Object)null);
    }

    @Override
    public TitanVertexQuery hasNot(String key) {
        return has(key,Cmp.EQUAL,(Object)null);
    }

    @Override
    public <T extends Comparable<?>> VertexCentricQueryBuilder interval(TitanKey key, T start, T end) {
        return interval(key.getName(),start,end);
    }

    @Override
    public <T extends Comparable<?>> VertexCentricQueryBuilder interval(String key, T start, T end) {
        addConstraint(key,Cmp.GREATER_THAN_EQUAL,start);
        return addConstraint(key,Cmp.LESS_THAN,end);
    }

    @Override
    @Deprecated
    public <T extends Comparable<T>> VertexCentricQueryBuilder has(String key, T value, Compare compare) {
        return addConstraint(key,TitanPredicate.Converter.convert(compare),value);
    }

    @Override
    public VertexCentricQueryBuilder types(TitanType... type) {
        for (TitanType t : type) type(t);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder labels(String... labels) {
        types.addAll(Arrays.asList(labels));
        return this;
    }

    @Override
    public VertexCentricQueryBuilder keys(String... keys) {
        types.addAll(Arrays.asList(keys));
        return this;
    }

    public VertexCentricQueryBuilder type(TitanType type) {
        return type(type.getName());
    }

    public VertexCentricQueryBuilder type(String type) {
        types.add(type);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder direction(Direction d) {
        Preconditions.checkNotNull(d);
        dir = d;
        return this;
    }

    public VertexCentricQueryBuilder includeHidden() {
        includeHidden = true;
        return this;
    }

    @Override
    public VertexCentricQueryBuilder limit(int limit) {
        Preconditions.checkArgument(limit>=0,"Limit must be non-negative [%s]",limit);
        this.limit = limit;
        return this;
    }
}
