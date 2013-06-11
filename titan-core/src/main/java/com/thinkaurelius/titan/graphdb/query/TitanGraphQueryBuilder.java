package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAnd;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.stats.ObjectAccumulator;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanGraphQueryBuilder implements TitanGraphQuery, QueryOptimizer<StandardElementQuery>  {

    private static final Logger log = LoggerFactory.getLogger(TitanGraphQueryBuilder.class);


    private static final List<KeyAtom<TitanKey>> INVALID = ImmutableList.of();

    private final StandardTitanTx tx;
    private List<KeyAtom<TitanKey>> conditions;
    private int limit = Query.NO_LIMIT;

    public TitanGraphQueryBuilder(StandardTitanTx tx) {
        Preconditions.checkNotNull(tx);
        this.tx=tx;
        this.conditions = Lists.newArrayList();
    }

    private boolean isInvalid() {
        return conditions==INVALID || limit==0;
    }

    @Override
    public TitanGraphQuery has(String key, Relation relation, Object condition) {
        Preconditions.checkNotNull(key);
        TitanType type = tx.getType(key);
        if (type==null || !(type instanceof TitanKey)) {
            if (tx.getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
                conditions = INVALID;
                return this;
            } else {
                throw new IllegalArgumentException("Unknown or invalid property key: " + key);
            }
        } else return has((TitanKey) type, relation, condition);
    }

    @Override
    public TitanGraphQuery has(String key, Compare relation, Object value) {
        return has(key,Cmp.convert(relation),value);
    }

    @Override
    public TitanGraphQuery has(TitanKey key, Relation relation, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(relation);
        condition=AttributeUtil.verifyAttributeQuery(key,condition);
        Preconditions.checkArgument(relation.isValidCondition(condition),"Invalid condition: %s",condition);
        Preconditions.checkArgument(relation.isValidDataType(key.getDataType()),"Invalid data type for condition: %s",key.getDataType());
        if (conditions!=INVALID) {
            conditions.add(new KeyAtom<TitanKey>(key,relation,condition));
        }
        return this;
    }

    @Override
    public <T extends Comparable<T>> TitanGraphQuery has(String s, T t, Compare compare) {
        return has(s,Cmp.convert(compare),t);
    }

    @Override
    public <T extends Comparable<T>> TitanGraphQuery has(String key, Compare compare, T value) {
        return has(key, compare, (Object)value);
    }

    @Override
    public <T extends Comparable<T>> TitanGraphQuery interval(String s, T t, T t2) {
        return has(s,Cmp.INTERVAL,new Interval<T>(t,t2));
    }

    private StandardElementQuery constructQuery(StandardElementQuery.Type elementType) {
        Preconditions.checkNotNull(elementType);
        return new StandardElementQuery(elementType,KeyAnd.of(conditions.toArray(new KeyAtom[conditions.size()])),limit,null);
    }

    @Override
    public Iterable<Vertex> vertices() {
        if (isInvalid()) return ImmutableList.of();
        StandardElementQuery query = constructQuery(StandardElementQuery.Type.VERTEX);
        return Iterables.filter(new QueryProcessor<StandardElementQuery,TitanElement>(query,tx.elementProcessor,this),Vertex.class);
    }

    @Override
    public Iterable<Edge> edges() {
        if (isInvalid()) return ImmutableList.of();
        StandardElementQuery query = constructQuery(StandardElementQuery.Type.EDGE);
        return Iterables.filter(new QueryProcessor<StandardElementQuery,TitanElement>(query,tx.elementProcessor,this),Edge.class);
    }


    @Override
    public TitanGraphQueryBuilder limit(long max) {
        Preconditions.checkArgument(max>=0,"Non-negative limit expected: %s",max);
        Preconditions.checkArgument(max<=Integer.MAX_VALUE,"Limit expected to be smaller or equal than [%s] but given %s",Integer.MAX_VALUE,limit);
        this.limit=(int)max;
        return this;
    }

    @Override
    public List<StandardElementQuery> optimize(StandardElementQuery query) {
        if (query.isInvalid()) return ImmutableList.of();
        //Find most suitable index
        ObjectAccumulator<String> opt = new ObjectAccumulator<String>(5);
        KeyCondition<TitanKey> condition = query.getCondition();
        if (condition.hasChildren()) {
            Preconditions.checkArgument(condition instanceof KeyAnd);
            for (KeyCondition<TitanKey> c : condition.getChildren()) {
                KeyAtom<TitanKey> atom = (KeyAtom<TitanKey>)c;
                if (atom.getCondition()==null) continue; //Cannot answer those with index
                for (String index : atom.getKey().getIndexes(query.getType().getElementType())) {
                    if (tx.getGraph().getIndexInformation(index).supports(atom.getKey().getDataType(),atom.getRelation()))
                        opt.incBy(index,1);
                }
            }
        }
        String bestIndex = opt.getMaxObject();
        log.debug("Best index for query [{}]: {}",query,bestIndex);
        if (bestIndex!=null) return ImmutableList.of(new StandardElementQuery(query,bestIndex));
        else return ImmutableList.of(query);
    }

    @Override
    public GraphQuery has(String key, Object... values) {
        throw new UnsupportedOperationException("has(key, values) not yet implemented");
    }

    @Override
    public GraphQuery hasNot(String key, Object... values) {
        throw new UnsupportedOperationException("hasNot(key, values) not yet implemented");
    }

    @Override
    public GraphQuery limit(long skip, long take) {
        throw new UnsupportedOperationException("limit(skip, take) not yet implemented");
    }
}
