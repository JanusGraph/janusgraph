// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.util.datastructures;

import java.util.ArrayList;
import java.util.List;

public class Combinatorics {

    public static <T> List<List<T>> cartesianProduct(List<List<T>> components) {
        return cartesianProduct(0, components);
    }

    private static <T> List<List<T>> cartesianProduct(int i, List<List<T>> components) {
        List<List<T>> result = new ArrayList<>();

        if (i == components.size()) {
            // reached end, stop recursion
            result.add(new ArrayList<>());
            return result;
        }

        List<List<T>> partialResult = cartesianProduct(i + 1, components);
        for (int j = 0; j < components.get(i).size(); j++) {
            for (List<T> entry : partialResult) {
                List<T> merged = new ArrayList<>();
                merged.add(components.get(i).get(j));
                merged.addAll(entry);
                result.add(merged);
            }
        }

        return result;
    }
}
