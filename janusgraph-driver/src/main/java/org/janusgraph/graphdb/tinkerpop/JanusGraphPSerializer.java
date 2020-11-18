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

import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;

/**
 * Serialize Janus Graph Predicates.
 * <p>
 * Since all predicates end up being an instance of P and the TinkerPop P
 * serializer is not extensible, it is copied here and the JanusGraph predicates
 * are added.
 */
public class JanusGraphPSerializer extends Serializer<JanusGraphP> {

    @Override
    public void write(Kryo kryo, Output output, JanusGraphP p) {
        output.writeString(p.getBiPredicate().toString());
        kryo.writeClassAndObject(output, p.getValue());
    }

    @Override
    public JanusGraphP read(Kryo kryo, Input input, Class<JanusGraphP> aClass) {
        final String predicate = input.readString();
        final Object value = kryo.readClassAndObject(input);

        return createPredicateWithValue(predicate, value);
    }

    public static boolean checkForJanusGraphPredicate(String predicateName) {
        switch (predicateName) {
            case "geoIntersect":
            case "geoDisjoint":
            case "geoWithin":
            case "geoContains":
            case "textContains":
            case "textContainsFuzzy":
            case "textContainsPrefix":
            case "textContainsRegex":
            case "textFuzzy":
            case "textPrefix":
            case "textRegex":
                return true;
            default:
                return false;
        }
    }

    public static JanusGraphP createPredicateWithValue(String predicateName, Object value) {
        switch (predicateName) {
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
                throw new UnsupportedOperationException("Matched predicate {" + predicateName + "} is not support by JanusGraphPSerializer");
        }
    }
}
