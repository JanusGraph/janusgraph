// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

public class CollectionsUtil {

    public static <T, R> ArrayList<R> toArrayList(Collection<T> collection, Function<? super T, ? extends R> mapFunction){
        ArrayList<R> result = new ArrayList<>(collection.size());
        for(T e : collection){
            result.add(mapFunction.apply(e));
        }
        return result;
    }

}
