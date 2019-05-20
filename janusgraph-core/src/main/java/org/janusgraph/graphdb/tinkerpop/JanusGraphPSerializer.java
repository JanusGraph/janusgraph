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

package org.janusgraph.graphdb.tinkerpop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serialize Janus Graph Predicates.
 * 
 * Since all predicates end up being an instance of P and the TinkerPop P
 * serializer is not extensible, it is copied here and the JanusGraph predicates
 * are added.
 *
 */
public class JanusGraphPSerializer extends Serializer<P> {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphPSerializer.class);

    @Override
    public void write(Kryo kryo, Output output, P p) {
        output.writeString(
                p instanceof ConnectiveP ? (p instanceof AndP ? "and" : "or") : p.getBiPredicate().toString());
        if (p instanceof ConnectiveP || p.getValue() instanceof Collection) {
            output.writeByte((byte) 0);
            final Collection<?> coll = p instanceof ConnectiveP ? ((ConnectiveP<?>) p).getPredicates()
                    : (Collection) p.getValue();
            output.writeInt(coll.size());
            coll.forEach(v -> kryo.writeClassAndObject(output, v));
        } else {
            output.writeByte((byte) 1);
            kryo.writeClassAndObject(output, p.getValue());
        }
    }

    @Override
    public P read(Kryo kryo, Input input, Class<P> aClass) {
        final String predicate = input.readString();
        final boolean isCollection = input.readByte() == (byte) 0;
        final Object value;
        if (isCollection) {
            value = new ArrayList();
            final int size = input.readInt();
            for (int ix = 0; ix < size; ix++) {
                ((List) value).add(kryo.readClassAndObject(input));
            }
        } else {
            value = kryo.readClassAndObject(input);
        }

        try {

            if (value instanceof Collection) {
                switch (predicate) {
                case "and":
                    return new AndP((List<P>) value);
                case "or":
                    return new OrP((List<P>) value);
                case "between":
                    return P.between(((List) value).get(0), ((List) value).get(1));
                case "inside":
                    return P.inside(((List) value).get(0), ((List) value).get(1));
                case "outside":
                    return P.outside(((List) value).get(0), ((List) value).get(1));
                case "within":
                    return P.within((Collection) value);
                case "without":
                    return P.without((Collection) value);
                default:
                    return (P) P.class.getMethod(predicate, Collection.class).invoke(null, (Collection) value);
                }
            } else {
                switch (predicate) {
                case "geoIntersect":
                    return Geo.geoIntersect(value);
                case "geoDisjoint":
                    return Geo.geoDisjoint(value);
                case "geoWithin":
                    return Geo.geoWithin(value);
                case "geoContains":
                    return Geo.geoContains(value);
                case "textContains":
                    return Text.textContains(value);
                case "textContainsFuzzy":
                    return Text.textContainsFuzzy(value);
                case "textContainsPrefix":
                    return Text.textContainsPrefix(value);
                case "textContainsRegex":
                    return Text.textContainsRegex(value);
                case "textFuzzy":
                    return Text.textFuzzy(value);
                case "textPrefix":
                    return Text.textPrefix(value);
                case "textRegex":
                    return Text.textRegex(value);
                default:
                    return (P) P.class.getMethod(predicate, Object.class).invoke(null, value);
                }
            }

        } catch (final Exception e) {
            log.info("Couldn't deserialize class: " + aClass + ", predicate: " + predicate + ", isCollection: "
                    + isCollection + ",value: " + value, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
