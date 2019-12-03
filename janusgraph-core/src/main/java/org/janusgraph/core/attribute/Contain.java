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

package org.janusgraph.core.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.graphdb.query.JanusGraphPredicate;

import java.util.Collection;

/**
 * Comparison relations for text objects.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Contain implements JanusGraphPredicate {

    /**
     * Whether an element is in a collection
     */
    IN {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            Collection col = (Collection) condition;
            return col.contains(value);
        }

        @Override
        public JanusGraphPredicate negate() {
            return NOT_IN;
        }
    },

    /**
     * Whether an element is not in a collection
     */
    NOT_IN {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            Collection col = (Collection) condition;
            return !col.contains(value);
        }

        @Override
        public JanusGraphPredicate negate() {
            return IN;
        }

    };

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        return true;
    }

    @Override
    public boolean isValidCondition(Object condition) {
        return condition instanceof Collection;
    }

    @Override
    public boolean hasNegation() {
        return true;
    }

    @Override
    public boolean isQNF() {
        return false;
    }


}
