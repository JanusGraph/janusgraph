// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.core.attribute;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.janusgraph.graphdb.query.JanusGraphPredicate;

/**
 * A {@link JanusGraphPredicate} that just wraps a TinkerPop {@link Text} predicate.
 *
 * This enables JanusGraph to use TinkerPop {@link Text} predicates in places where it expects a
 * {@link JanusGraphPredicate}.
 */
public class TinkerPopTextWrappingPredicate implements JanusGraphPredicate {
    private Text internalPredicate;

    public TinkerPopTextWrappingPredicate(Text wrappedPredicate) {
        internalPredicate = wrappedPredicate;
    }

    @Override
    public boolean isValidCondition(Object condition) {
        return condition instanceof String && StringUtils.isNoneBlank(condition.toString());
    }

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        return clazz.equals(String.class);
    }

    @Override
    public boolean hasNegation() {
        return true;
    }

    @Override
    public JanusGraphPredicate negate() {
        return new TinkerPopTextWrappingPredicate(internalPredicate.negate());
    }

    @Override
    public boolean isQNF() {
        return true;
    }

    @Override
    public boolean test(Object value, Object condition) {
        return internalPredicate.test(value.toString(), (String) condition);
    }
}
