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

package org.janusgraph.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StringUtils {

    public static String join(Map<?, ?> elements, String keyValueSeparator, String entriesSeparator){
        if(elements == null){
            return "";
        }

        Iterator<? extends Map.Entry<?, ?>> it = elements.entrySet().iterator();
        if(it.hasNext()){
            Map.Entry<?, ?> entry = it.next();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(entry.getKey()).append(keyValueSeparator).append(entry.getValue());
            while (it.hasNext()){
                entry = it.next();
                stringBuilder.append(entriesSeparator).append(entry.getKey()).append(keyValueSeparator).append(entry.getValue());
            }
            return stringBuilder.toString();
        }
        return "";
    }

    public static String join(Object[] elements, String separator){
        if(elements == null || elements.length == 0){
            return "";
        }
        return join(Stream.of(elements), separator);
    }

    public static String join(Collection<?> elements, String separator){
        if(elements == null){
            return "";
        }
        return join(elements.stream(), separator);
    }

    private static <E> String join(Stream<E> elements, String separator) {
        return elements.map(String::valueOf).collect(Collectors.joining(separator));
    }
}
