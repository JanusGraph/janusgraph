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

package org.janusgraph.graphdb.database.serialize;

import org.janusgraph.core.attribute.Geoshape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class AttributeUtils {

    private static final Logger log = LoggerFactory.getLogger(AttributeUtils.class);


    public static boolean isWholeNumber(Number n) {
        return isWholeNumber(n.getClass());
    }

    public static boolean isWholeNumber(Class<?> clazz) {
        return clazz.equals(Long.class) || clazz.equals(Integer.class) ||
                clazz.equals(Short.class) || clazz.equals(Byte.class);
    }

    public static boolean isDecimal(Class<?> clazz) {
        return clazz.equals(Double.class) || clazz.equals(Float.class);
    }

    public static boolean isString(Object o) {
        return isString(o.getClass());
    }

    public static boolean isString(Class<?> clazz) {
        return clazz.equals(String.class);
    }

    public static boolean isGeo(Class<?> clazz) {
        return clazz.equals(Geoshape.class);
    }

    /**
     * Compares two values of a key in the order in which JanusGraph holds them: by their natural order, except two
     * UUIDs, which compare by their bits as unsigned numbers ({@link #compareUnsigned(UUID, UUID)}), as their string
     * forms and their bytes in the storage backend do.
     *
     * @throws ClassCastException if the values aren't comparable with each other
     */
    public static int compareValues(Object a, Object b) {
        if (a instanceof UUID && b instanceof UUID) {
            return compareUnsigned((UUID) a, (UUID) b);
        }
        return ((Comparable) a).compareTo(b);
    }

    /**
     * Compares two UUIDs by their bits as unsigned numbers, the most significant half first. That is the order of their
     * string forms, of lower-case hexadecimal digits, in which Elasticsearch, OpenSearch and Solr sort them, and of
     * their bytes, in which the storage backend holds them as sort keys. {@link UUID#compareTo} compares each half as a
     * signed number instead: where, in the first half in which two UUIDs differ, one starts with a digit from 8 to f
     * and the other with one from 0 to 7, it puts the first before the second.
     */
    public static int compareUnsigned(UUID u1, UUID u2) {
        final int cmp = Long.compareUnsigned(u1.getMostSignificantBits(), u2.getMostSignificantBits());
        return cmp != 0 ? cmp : Long.compareUnsigned(u1.getLeastSignificantBits(), u2.getLeastSignificantBits());
    }

    /**
     * Compares the two elements like {@link java.util.Comparator#compare(Object, Object)} but returns
     * null in case the two elements are not comparable.
     *
     * @param a
     * @param b
     * @return
     */
    public static Integer compare(Object a, Object b) {
        if (a==b) return 0;
        if (a==null || b==null) return null;
        assert a!=null && b!=null;
        if (a instanceof Number && b instanceof Number) {
            Number an = (Number)a;
            Number bn = (Number)b;
            if (Double.isNaN(an.doubleValue()) || Double.isNaN(bn.doubleValue())) {
                if (Double.isNaN(an.doubleValue()) && Double.isNaN(bn.doubleValue())) return 0;
                else return null;
            } else {
                if (an.doubleValue()==bn.doubleValue()) {
                    // Long.compare(long,long) is only available since Java 1.7
                    //return Long.compare(an.longValue(),bn.longValue());
                    return Long.compare(an.longValue(), bn.longValue());
                } else {
                    return Double.compare(an.doubleValue(),bn.doubleValue());
                }
            }
        } else {
            try {
                return compareValues(a, b);
            } catch (Throwable e) {
                log.debug("Could not compare elements: {} - {}",a,b);
                return null;
            }
        }
    }
}
