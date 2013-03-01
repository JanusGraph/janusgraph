package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAnd;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.stats.ObjectAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ElementQueryBuilder implements ElementQuery, QueryOptimizer<StandardElementQuery>  {

    private static final Logger log = LoggerFactory.getLogger(ElementQueryBuilder.class);


    private static final List<KeyAtom<TitanKey>> INVALID = ImmutableList.of();

    private final StandardTitanTx tx;
    private List<KeyAtom<TitanKey>> conditions;

    public ElementQueryBuilder(StandardTitanTx tx) {
        Preconditions.checkNotNull(tx);
        this.tx=tx;
        this.conditions = Lists.newArrayList();
    }

    @Override
    public ElementQuery and(String key, Relation relation, Object condition) {
        Preconditions.checkNotNull(key);
        TitanType type = tx.getType(key);
        if (type==null || !(type instanceof TitanKey)) {
            if (tx.getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
                condition = INVALID;
                return this;
            } else {
                throw new IllegalArgumentException("Unknown or invalid property key: " + key);
            }
        } else return and((TitanKey)type,relation,condition);
    }

    @Override
    public ElementQuery and(TitanKey key, Relation relation, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(relation);
        if (condition!=null) condition=AttributeUtil.verifyAttribute(key,condition);
        Preconditions.checkArgument(relation.isValidCondition(condition),"Invalid condition: %s",condition);
        Preconditions.checkArgument(relation.isValidDataType(key.getDataType()),"Invalid data type for condition: %s",key.getDataType());
        if (conditions!=INVALID) {
            conditions.add(new KeyAtom<TitanKey>(key,relation,condition));
        }
        return this;
    }

    private StandardElementQuery constructQuery(StandardElementQuery.Type elementType) {
        Preconditions.checkNotNull(elementType);
        return new StandardElementQuery(elementType,KeyAnd.of(conditions.toArray(new KeyAtom[conditions.size()])),null);
    }

    @Override
    public Iterable<TitanVertex> getVertices() {
        if (conditions==INVALID) return ImmutableList.of();
        else if (conditions.isEmpty()) return Iterables.filter(tx.getVertices(), TitanVertex.class);
        StandardElementQuery query = constructQuery(StandardElementQuery.Type.VERTEX);
        return Iterables.filter(new QueryProcessor<StandardElementQuery,TitanElement>(query,tx.elementProcessor,this),TitanVertex.class);
    }

    @Override
    public Iterable<TitanEdge> getEdges() {
        if (conditions==INVALID) return ImmutableList.of();
        else if (conditions.isEmpty()) return Iterables.filter(tx.getEdges(),TitanEdge.class);
        StandardElementQuery query = constructQuery(StandardElementQuery.Type.EDGE);
        return Iterables.filter(new QueryProcessor<StandardElementQuery,TitanElement>(query,tx.elementProcessor,this),TitanEdge.class);
    }

    @Override
    public List<StandardElementQuery> optimize(StandardElementQuery query) {
        if (query.isInvalid()) return ImmutableList.of();
        //Find most suitable index
        ObjectAccumulator<String> opt = new ObjectAccumulator<String>(5);
        KeyCondition<TitanKey> condition = query.getCondition();
        //ASSUMPTION: query is an AND of KeyAtom
        Preconditions.checkArgument(condition instanceof KeyAnd);
        Preconditions.checkArgument(condition.hasChildren());
        for (KeyCondition<TitanKey> c : condition.getChildren()) {
            KeyAtom<TitanKey> atom = (KeyAtom<TitanKey>)c;
            for (String index : atom.getKey().getIndexes(query.getType().getElementType())) {
                if (tx.getGraph().getIndexInformation(index).supports(atom.getKey().getDataType(),atom.getRelation()))
                    opt.incBy(index,1);
            }
        }
        String bestIndex = opt.getMaxObject();
        log.debug("Best index for query [{}]: {}",query,bestIndex);
        if (bestIndex!=null) return ImmutableList.of(new StandardElementQuery(query,bestIndex));
        else return ImmutableList.of(query);
    }
}
