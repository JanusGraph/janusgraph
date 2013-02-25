package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAnd;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VertexCentricQueryBuilder implements TitanQuery {

    private final InternalVertex vertex;

    private Direction dir;
    private Set<String> types;
    private TypeGroup group;
    private List<KeyAtom<String>> constraints;
    private boolean includeHidden;
    private int limit = Query.NO_LIMIT;


    public VertexCentricQueryBuilder(InternalVertex v) {
        Preconditions.checkNotNull(v);
        this.vertex=v;

        dir = Direction.BOTH;
        types = new HashSet<String>(4);
        group = null;
        constraints = Lists.newArrayList();
        includeHidden = false;
    }

    public static final VertexCentricQueryBuilder queryAll(InternalVertex node) {
        return new VertexCentricQueryBuilder(node).includeHidden();
    }

    private final TitanType getType(String typeName) {
        TitanType t = vertex.tx().getType(typeName);
        if (t == null && !vertex.tx().getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return t;
    }

    private final TitanKey getKey(String keyName) {
        TitanType t = getType(keyName);
        if (t instanceof TitanKey) return (TitanKey) t;
        else throw new IllegalArgumentException("Provided name does not represent a key: " + keyName);
    }

	/* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    private VertexCentricQuery constructQuery(RelationType returnType) {
        Preconditions.checkNotNull(returnType);
        Preconditions.checkArgument(limit>0);
        Preconditions.checkArgument(dir!=null);

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
                    if (group!=null && !group.equals(t.getGroup()))
                        throw new IllegalArgumentException("Given type conflicts with group assignment: " + type);
                    if (t.isPropertyKey() && returnType!= RelationType.PROPERTY)
                        throw new IllegalArgumentException("Querying for edges but including a property key: " + t.getName());
                    if (t.isEdgeLabel() && returnType!= RelationType.EDGE)
                        throw new IllegalArgumentException("Querying for properties but including an edge label: " + t.getName());
                }
            }
            if (ts.isEmpty()) return VertexCentricQuery.INVALID;
            group = null;
        }

        //check constraints
        KeyAtom<TitanType>[] c = new KeyAtom[constraints.size()];
        for (int i=0;i<constraints.size();i++) {
            KeyAtom<String> atom = constraints.get(i);
            TitanType t = getType(atom.getKey());
            if (t==null) return VertexCentricQuery.INVALID;
            Object condition = atom.getCondition();
            Relation relation = atom.getRelation();
            //Check condition
            Preconditions.checkArgument(relation.isValidCondition(condition),"Invalid condition onf key [%s]: %s",t.getName(),condition);
            if (t.isPropertyKey()) {
                if (condition!=null) condition = AttributeUtil.verifyAttribute((TitanKey)t,condition);
                Preconditions.checkArgument(relation.isValidDataType(((TitanKey)t).getDataType()),"Invalid data type for condition");
            } else { //t.isEdgeLabel()
                Preconditions.checkArgument(((TitanLabel)t).isUnidirected() && (condition instanceof TitanVertex));
            }
            c[i]=new KeyAtom<TitanType>(t, relation, condition);
        }
        return new VertexCentricQuery(vertex,dir,ts.toArray(new TitanType[ts.size()]),group,KeyAnd.of(c),includeHidden,limit,returnType);
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
        return Iterables.filter(relations(RelationType.EDGE),TitanProperty.class);
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

    private VertexCentricQueryBuilder addConstraint(String type, Relation rel, Object value) {
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
    public VertexCentricQueryBuilder has(TitanLabel label, TitanVertex value) {
        return has(label.getName(),value);
    }

    @Override
    public VertexCentricQueryBuilder has(String type, Object value) {
        return addConstraint(type,Cmp.EQUAL,value);
    }

    @Override
    public <T extends Comparable<T>> VertexCentricQueryBuilder interval(TitanKey key, T start, T end) {
        return interval(key.getName(),start,end);
    }

    @Override
    public <T extends Comparable<T>> VertexCentricQueryBuilder interval(String key, T start, T end) {
        addConstraint(key,Cmp.GREATER_THAN_EQUAL,start);
        return addConstraint(key,Cmp.LESS_THAN_EQUAL,end);
    }

    @Override
    public <T extends Comparable<T>> VertexCentricQueryBuilder has(String key, T value, Compare compare) {
        return addConstraint(key,Cmp.convert(compare),value);
    }

    public <T extends Comparable<T>> VertexCentricQueryBuilder has(TitanKey key, T value, Compare compare) {
        return has(key.getName(),value,compare);
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
    public VertexCentricQueryBuilder group(TypeGroup group) {
        Preconditions.checkNotNull(group);
        this.group = group;
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
    public VertexCentricQueryBuilder limit(long limit) {
        Preconditions.checkArgument(limit>0,"Limit must be positive [%s]",limit);
        Preconditions.checkArgument(limit<Integer.MAX_VALUE,"Limit is too large [%s]",limit);
        this.limit = (int)limit;
        return this;
    }



}
