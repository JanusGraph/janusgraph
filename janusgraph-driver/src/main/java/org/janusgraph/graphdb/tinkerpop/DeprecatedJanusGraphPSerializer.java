// Copyright 2020 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@Deprecated
public class DeprecatedJanusGraphPSerializer implements SerializerShim<P> {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphPSerializer.class);
    private final SerializerShim<P> pSerializerShim;

    public DeprecatedJanusGraphPSerializer(SerializerShim<P> pSerializerShim) {
        this.pSerializerShim = pSerializerShim;
    }

    @Override
    public <O extends OutputShim> void write(KryoShim<?, O> kryo, O output, P p) {
        pSerializerShim.write(kryo, output, p);

    }

    @Override
    public <I extends InputShim> P read(KryoShim<I, ?> kryo, I input, Class<P> aClass) {
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
            return createPredicateWithValue(predicate, value);
        } catch (final Exception e) {
            log.info("Couldn't deserialize class: " + aClass + ", predicate: " + predicate + ", isCollection: "
                + isCollection + ",value: " + value, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static P createPredicateWithValue(String predicate, Object value) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        if (JanusGraphPSerializer.checkForJanusGraphPredicate(predicate)){
            return JanusGraphPSerializer.createPredicateWithValue(predicate, value);
        }
        if (!predicate.equals("and") && !predicate.equals("or")) {
            if (value instanceof Collection) {
                switch (predicate) {
                    case "between":
                        return P.between(((List) value).get(0), ((List) value).get(1));
                    case "inside":
                        return P.inside(((List) value).get(0), ((List) value).get(1));
                    case "outside":
                        return P.outside(((List) value).get(0), ((List) value).get(1));
                    case "within":
                        return P.within((Collection) value);
                    default:
                        return predicate.equals("without") ? P.without((Collection) value) : (P) P.class.getMethod(predicate, Collection.class).invoke(null, value);
                }
            } else {
                return (P) P.class.getMethod(predicate, Object.class).invoke(null, value);
            }
        } else {
            return (P) (predicate.equals("and") ? new AndP((List) value) : new OrP((List) value));
        }
    }
}
