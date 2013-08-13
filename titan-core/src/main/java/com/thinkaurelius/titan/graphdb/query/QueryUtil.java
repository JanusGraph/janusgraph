package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.MultiCondition;
import com.thinkaurelius.titan.graphdb.query.condition.Or;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class QueryUtil {

    public static final TitanProperty queryHiddenUniqueProperty(InternalVertex vertex, TitanKey key) {
        assert ((InternalType) key).isHidden() : "Expected hidden property key";
        assert key.isUnique(Direction.OUT) : "Expected functional property  type";
        return Iterables.getOnlyElement(
                vertex.query().
                        includeHidden().
                        type(key).
                        properties(), null);
    }

    public static final Iterable<TitanRelation> queryAll(InternalVertex vertex) {
        return vertex.query().includeHidden().relations();
    }

    public static final int adjustLimitForTxModifications(StandardTitanTx tx, And<? extends TitanElement> conditions, int limit) {
        Preconditions.checkArgument(limit>0 && limit<10000000,"Invalid limit: %s",limit); //To make sure limit computation does not overflow
        if (conditions.hasChildren()) limit = (limit*3)/2+1;
        if (tx.hasModifications()) limit += 5;
        return limit;
    }

    private static final InternalType getType(StandardTitanTx tx, String typeName) {
        TitanType t = tx.getType(typeName);
        if (t == null && !tx.getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return (InternalType)t;
    }

    public static final<E extends TitanElement> boolean prepareConstraints(StandardTitanTx tx, And<E> conditions, List<PredicateCondition<String,E>> constraints) {
        for (int i=0;i<constraints.size();i++) {
            PredicateCondition<String,E> atom = constraints.get(i);
            TitanType type = getType(tx,atom.getKey());
            if (type==null) {
                if (atom.getPredicate()== Cmp.EQUAL && atom.getCondition()==null) continue; //Ignore condition
                else return false;
            }
            Object value = atom.getCondition();
            TitanPredicate predicate = atom.getPredicate();
            Preconditions.checkArgument(predicate.isValidCondition(value),"Invalid condition: %s",value);
            if (type.isPropertyKey()) {
                Preconditions.checkArgument(predicate.isValidDataType(((TitanKey)type).getDataType()),"Data type of key is not compatible with condition");
            } else { //its a label
                Preconditions.checkArgument(((TitanLabel)type).isUnidirected());
                Preconditions.checkArgument(predicate.isValidDataType(TitanVertex.class),"Data type of key is not compatible with condition");
            }


            if (predicate==Contain.NOT_IN) { //Rewrite contains relationship
                Collection values = (Collection)value;
                for (Object invalue : values) addConstraint(type,Cmp.NOT_EQUAL,invalue,conditions);
            } else if (predicate==Contain.IN && ((Collection)value).size()==1) {
                addConstraint(type,Cmp.EQUAL,((Collection)value).iterator().next(),conditions);
            } else {
                addConstraint(type,predicate,value,conditions);
            }


        }
        return true;
    }

    private static final<E extends TitanElement> void addConstraint(TitanType type, TitanPredicate predicate, Object value, MultiCondition<E> conditions) {
        if (predicate instanceof Contain) {
            Collection values = (Collection)value;
            Collection newValues = new ArrayList(values.size());
            for (Object v : values) {
                if (type.isPropertyKey()) {
                    v = AttributeUtil.verifyAttributeQuery((TitanKey) type, v); //TODO: replace by AttributeSerializer based handling
                } else { //t.isEdgeLabel()
                    Preconditions.checkArgument(v instanceof TitanVertex);
                }
                newValues.add(v);
            }
            conditions.add(new PredicateCondition<TitanType,E>(type, predicate, newValues));
        } else {
            if (type.isPropertyKey()) {
                value = AttributeUtil.verifyAttributeQuery((TitanKey) type, value); //TODO: replace by AttributeSerializer based handling
            } else { //t.isEdgeLabel()
                Preconditions.checkArgument(value instanceof TitanVertex);
            }
            conditions.add(new PredicateCondition<TitanType,E>(type, predicate, value));
        }
    }


}
