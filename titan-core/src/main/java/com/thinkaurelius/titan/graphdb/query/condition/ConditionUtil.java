package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Function;
import com.thinkaurelius.titan.core.TitanElement;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConditionUtil {

    public static final<E extends TitanElement> Condition<E> literalTransformation(Condition<E> condition, Function<Condition<E>,Condition<E>> transformation) {
        if (condition.getType()== Condition.Type.LITERAL) {
            return transformation.apply(condition);
        } else if (condition instanceof Not) {
            return Not.of(literalTransformation(((Not) condition).getChild(),transformation));
        } else if (condition instanceof And) {
            And<E> newand = new And<E>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newand.add(literalTransformation(child,transformation));
            return newand;
        } else if (condition instanceof Or) {
            Or<E> newor = new Or<E>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newor.add(literalTransformation(child, transformation));
            return newor;
        } else throw new IllegalArgumentException("Unexpected condition type: " + condition);
    }

}
