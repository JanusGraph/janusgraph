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

import java.util.List;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
public class AndJanusPredicate extends ConnectiveJanusPredicate {

    private static final long serialVersionUID = -4249282297056862327L;

    public AndJanusPredicate(){
        super();
    }

    public AndJanusPredicate(final List<JanusGraphPredicate> predicates) {
        super(predicates);
    }

    @Override
    ConnectiveJanusPredicate getNewNegateInstance() {
        return new OrJanusPredicate();
    }

    @Override
    boolean isOr() {
        return false;
    }

    @Override
    public boolean isQNF() {
        for (final JanusGraphPredicate internalCondition : this) {
            if (!internalCondition.isQNF()){
                return false;
            }
        }
        return true;
    }
}
