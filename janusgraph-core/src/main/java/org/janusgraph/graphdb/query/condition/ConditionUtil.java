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

package org.janusgraph.graphdb.query.condition;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphElement;

import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Utility methods for transforming and inspecting {@link Condition}s.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConditionUtil {

    public static <E extends JanusGraphElement> Condition<E> literalTransformation(Condition<E> condition, final Function<Condition<E>,Condition<E>> transformation) {
        return transformation(condition,new Function<Condition<E>, Condition<E>>() {
            @Nullable
            @Override
            public Condition<E> apply(final Condition<E> cond) {
                if (cond.getType()== Condition.Type.LITERAL) return transformation.apply(cond);
                else return null;
            }
        });
    }

    public static <E extends JanusGraphElement> Condition<E> transformation(Condition<E> condition, Function<Condition<E>,Condition<E>> transformation) {
        Condition<E> transformed = transformation.apply(condition);
        if (transformed!=null) return transformed;
        //if transformed==null we go a level deeper
        if (condition.getType()== Condition.Type.LITERAL) {
            return condition;
        } else if (condition instanceof Not) {
            return Not.of(transformation(((Not) condition).getChild(), transformation));
        } else if (condition instanceof And) {
            final And<E> newAnd = new And<>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newAnd.add(transformation(child, transformation));
            return newAnd;
        } else if (condition instanceof Or) {
            final Or<E> newOr = new Or<>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newOr.add(transformation(child, transformation));
            return newOr;
        } else throw new IllegalArgumentException("Unexpected condition type: " + condition);
    }

    public static boolean containsType(Condition<?> condition, Condition.Type type) {
        if (condition.getType()==type) return true;
        else if (condition.numChildren()>0) {
            for (Condition child : condition.getChildren()) {
                if (containsType(child,type)) return true;
            }
        }
        return false;
    }

    public static <E extends JanusGraphElement> void traversal(Condition<E> condition, Predicate<Condition<E>> evaluator) {
        Preconditions.checkArgument(!evaluator.test(condition)
            || condition.getType() == Condition.Type.LITERAL
            || condition instanceof Not
            || condition instanceof MultiCondition, "Unexpected condition type: %s", condition);
        if (condition instanceof Not) {
            traversal(((Not) condition).getChild(), evaluator);
        } else if (condition instanceof MultiCondition) {
            condition.getChildren().forEach(child -> traversal(child, evaluator));
        }
    }

}
