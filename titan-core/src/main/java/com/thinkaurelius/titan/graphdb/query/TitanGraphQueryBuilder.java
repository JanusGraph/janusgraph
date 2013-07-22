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
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAnd;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import com.thinkaurelius.titan.graphdb.query.keycondition.TitanPredicate;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.stats.ObjectAccumulator;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
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
    public TitanGraphQuery has(String key, Predicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        TitanPredicate titanPredicate = TitanPredicate.Converter.convert(predicate);
        TitanType type = tx.getType(key);
        if (type==null || !(type instanceof TitanKey)) {
            if ( (titanPredicate==Cmp.EQUAL && condition==null) || (titanPredicate==Contain.NOT_IN) ||
                    (titanPredicate==Cmp.NOT_EQUAL && condition!=null) ) {
                //Trivially satisfied
                return this;
            }
            else if (tx.getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
                conditions = INVALID;
                return this;
            } else {
                throw new IllegalArgumentException("Unknown or invalid property key: " + key);
            }
        } else return has((TitanKey) type, titanPredicate, condition);
    }

//    @Override
//    public TitanGraphQuery has(String key, Compare relation, Object value) {
//        return has(key,relation,value);
//    }

    @Override
    public TitanGraphQuery has(TitanKey key, TitanPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        condition=AttributeUtil.verifyAttributeQuery(key,condition);
        Preconditions.checkArgument(predicate.isValidCondition(condition),"Invalid condition: %s",condition);
        Preconditions.checkArgument(predicate.isValidDataType(key.getDataType()),"Invalid data type for condition: %s",key.getDataType());
        if (conditions!=INVALID) {
            conditions.add(new KeyAtom<TitanKey>(key, predicate,condition));
        }
        return this;
    }

    @Override
    public TitanGraphQuery has(String key) {
        return has(key,Cmp.NOT_EQUAL,(Object)null);
    }

    @Override
    public TitanGraphQuery hasNot(String key) {
        return has(key,Cmp.EQUAL,(Object)null);
    }

    @Override
    @Deprecated
    public <T extends Comparable<T>> TitanGraphQuery has(String s, T t, Compare compare) {
        return has(s,compare,t);
    }
    
    @Override
    public TitanGraphQuery has(String key, Object value) {
        return has(key,Cmp.EQUAL,value);
    }

    @Override
    public TitanGraphQuery hasNot(String key, Object value) {
        return has(key,Cmp.NOT_EQUAL,value);
    }

    @Override
    public <T extends Comparable<?>> TitanGraphQuery interval(String s, T t1, T t2) {
        has(s,Cmp.GREATER_THAN_EQUAL,t1);
        return has(s,Cmp.LESS_THAN,t2);
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
    public TitanGraphQueryBuilder limit(final int max) {
        Preconditions.checkArgument(max>=0,"Non-negative limit expected: %s",max);
        this.limit=max;
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
                    if (tx.getGraph().getIndexInformation(index).supports(atom.getKey().getDataType(),atom.getTitanPredicate()))
                        opt.incBy(index,1);
                }
            }
        }
        String bestIndex = opt.getMaxObject();
        log.debug("Best index for query [{}]: {}",query,bestIndex);
        if (bestIndex!=null) return ImmutableList.of(new StandardElementQuery(query,bestIndex));
        else return ImmutableList.of(query);
    }
}
