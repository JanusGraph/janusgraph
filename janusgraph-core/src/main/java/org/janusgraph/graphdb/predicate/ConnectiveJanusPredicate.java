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

package org.janusgraph.graphdb.predicate;

import org.janusgraph.graphdb.query.JanusGraphPredicate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
public abstract class ConnectiveJanusPredicate extends ArrayList<JanusGraphPredicate> implements JanusGraphPredicate {

    private static final long serialVersionUID = 1558788908114391360L;

    public ConnectiveJanusPredicate(){
        super();
    }

    public ConnectiveJanusPredicate(final List<JanusGraphPredicate> predicates) {
        super(predicates);
    }

    abstract ConnectiveJanusPredicate getNewNegateInstance();

    abstract boolean isOr();

    @Override
    @SuppressWarnings("unchecked")
    public boolean isValidCondition(Object condition) {
        if (!(condition instanceof List) || ((List<?>)condition).size() != this.size()){
            return false;
        }
        final Iterator<Object> itConditions = ((List<Object>) condition).iterator();
        return this.stream().allMatch(internalCondition -> internalCondition.isValidCondition(itConditions.next()));
    }

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        return this.stream().allMatch(internalCondition -> internalCondition.isValidValueType(clazz));
    }

    @Override
    public boolean hasNegation() {
        return this.stream().allMatch(JanusGraphPredicate::hasNegation);
    }

    @Override
    public JanusGraphPredicate negate() {
        final ConnectiveJanusPredicate toReturn = getNewNegateInstance();
        this.stream().map(JanusGraphPredicate::negate).forEach(toReturn::add);
        return toReturn;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean test(Object value, Object condition) {
        if (!(condition instanceof List) || ((List<?>) condition).size() != this.size()){
            return false;
        }
        final Iterator<Object> itConditions = ((List<Object>) condition).iterator();
        return this.stream().anyMatch(internalCondition -> isOr() == internalCondition.test(value, itConditions.next())) == isOr();
    }
}
