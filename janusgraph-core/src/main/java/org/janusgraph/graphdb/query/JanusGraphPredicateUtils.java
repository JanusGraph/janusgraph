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

package org.janusgraph.graphdb.query;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.attribute.TinkerPopTextWrappingPredicate;
import org.janusgraph.graphdb.predicate.AndJanusPredicate;
import org.janusgraph.graphdb.predicate.ConnectiveJanusGraphP;
import org.janusgraph.graphdb.predicate.ConnectiveJanusPredicate;
import org.janusgraph.graphdb.predicate.OrJanusPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

public class JanusGraphPredicateUtils {

    /**
     * Convert Tinkerpop's comparison operators to JanusGraph's
     *
     * @param p Any predicate
     * @return A JanusGraphPredicate equivalent to the given predicate
     * @throws IllegalArgumentException if the given Predicate is unknown
     */
    public static JanusGraphPredicate convertInternal(BiPredicate p) {
        if (p instanceof JanusGraphPredicate) {
            return (JanusGraphPredicate)p;
        } else if (p instanceof Compare) {
            final Compare comp = (Compare)p;
            switch(comp) {
                case eq: return Cmp.EQUAL;
                case neq: return Cmp.NOT_EQUAL;
                case gt: return Cmp.GREATER_THAN;
                case gte: return Cmp.GREATER_THAN_EQUAL;
                case lt: return Cmp.LESS_THAN;
                case lte: return Cmp.LESS_THAN_EQUAL;
                default: throw new IllegalArgumentException("Unexpected comparator: " + comp);
            }
        } else if (p instanceof Contains) {
            final Contains con = (Contains)p;
            switch (con) {
                case within: return Contain.IN;
                case without: return Contain.NOT_IN;
                default: throw new IllegalArgumentException("Unexpected container: " + con);

            }
        } else return null;
    }

    public static JanusGraphPredicate convert(BiPredicate p) {
        final JanusGraphPredicate janusgraphPredicate = convertInternal(p);
        if (janusgraphPredicate==null) throw new IllegalArgumentException("JanusGraph does not support the given predicate: " + p);
        return janusgraphPredicate;
    }

    public static boolean supports(BiPredicate p) {
        return convertInternal(p)!=null;
    }

    public static HasContainer convert(final HasContainer container){
        if (!(container.getPredicate() instanceof ConnectiveP)) {
            return container;
        }
        final ConnectiveJanusPredicate connectivePredicate = instanceConnectiveJanusPredicate(container.getPredicate());
        return new HasContainer(container.getKey(), new ConnectiveJanusGraphP(connectivePredicate, convert(((ConnectiveP<?>) container.getPredicate()), connectivePredicate)));
    }

    public static ConnectiveJanusPredicate instanceConnectiveJanusPredicate(final P<?> predicate) {
        final ConnectiveJanusPredicate connectivePredicate;
        if (predicate.getClass().isAssignableFrom(AndP.class)){
            connectivePredicate = new AndJanusPredicate();
        } else if (predicate.getClass().isAssignableFrom(OrP.class)){
            connectivePredicate = new OrJanusPredicate();
        } else {
            throw new IllegalArgumentException("JanusGraph does not support the given predicate: " + predicate);
        }
        return connectivePredicate;
    }

    public static List<Object> convert(final ConnectiveP<?> predicate, final ConnectiveJanusPredicate connectivePredicate) {
        final List<Object> toReturn = new ArrayList<>();
        for (final P<?> p : predicate.getPredicates()){
            if (p instanceof ConnectiveP) {
                final ConnectiveJanusPredicate subPredicate = instanceConnectiveJanusPredicate(p);
                toReturn.add(convert((ConnectiveP<?>)p, subPredicate));
                connectivePredicate.add(subPredicate);
            } else if (p.getBiPredicate() instanceof Text) {
                Text text = (Text) p.getBiPredicate();
                connectivePredicate.add(new TinkerPopTextWrappingPredicate(text));
                toReturn.add(p.getValue());
            } else {
                connectivePredicate.add(JanusGraphPredicateUtils.convert(p.getBiPredicate()));
                toReturn.add(p.getValue());
            }
        }
        return toReturn;
    }
}
