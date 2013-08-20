package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.TitanElement;

import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConditionUtil {

    public static final<E extends TitanElement> Condition<E> literalTransformation(Condition<E> condition, final Function<Condition<E>,Condition<E>> transformation) {
        return transformation(condition,new Function<Condition<E>, Condition<E>>() {
            @Nullable
            @Override
            public Condition<E> apply(@Nullable Condition<E> cond) {
                if (cond.getType()== Condition.Type.LITERAL) return transformation.apply(cond);
                else return null;
            }
        });
    }

    public static final<E extends TitanElement> Condition<E> transformation(Condition<E> condition, Function<Condition<E>,Condition<E>> transformation) {
        Condition<E> transformed = transformation.apply(condition);
        if (transformed!=null) return transformed;
        //if transformed==null we go a level deeper
        if (condition.getType()== Condition.Type.LITERAL) {
            return condition;
        } else if (condition instanceof Not) {
            return Not.of(transformation(((Not) condition).getChild(), transformation));
        } else if (condition instanceof And) {
            And<E> newand = new And<E>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newand.add(transformation(child, transformation));
            return newand;
        } else if (condition instanceof Or) {
            Or<E> newor = new Or<E>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newor.add(transformation(child, transformation));
            return newor;
        } else throw new IllegalArgumentException("Unexpected condition type: " + condition);
    }

    public static final boolean containsType(Condition<?> condition, Condition.Type type) {
        if (condition.getType()==type) return true;
        else if (condition.numChildren()>0) {
            for (Condition child : condition.getChildren()) {
                if (containsType(child,type)) return true;
            }
        }
        return false;
    }

    public static final<E extends TitanElement> void traversal(Condition<E> condition, Predicate<Condition<E>> evaluator) {
        if (!evaluator.apply(condition)) return; //Abort if the evaluator returns false

        if (condition.getType()== Condition.Type.LITERAL) {
            return;
        } else if (condition instanceof Not) {
            traversal(((Not) condition).getChild(), evaluator);
        } else if (condition instanceof MultiCondition) {
            for (Condition<E> child : condition.getChildren()) traversal(child, evaluator);
        } else throw new IllegalArgumentException("Unexpected condition type: " + condition);
    }


}
