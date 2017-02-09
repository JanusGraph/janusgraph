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

package org.janusgraph.util.stats;


import com.carrotsearch.hppc.IntCollection;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleHashMap;

/**
 * Count relative integer frequencies
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IntegerDoubleFrequency {

    private final IntDoubleMap counts;
    private double total;

    public IntegerDoubleFrequency() {
        counts = new IntDoubleHashMap();
        total = 0;
    }

    public void addValue(int value, double amount) {
        counts.put(value, amount + counts.get(value));
        total += amount;
    }

    public IntCollection getValues() {
        return counts.keys();
    }

    public double getCount(int value) {
        return counts.get(value);
    }

    public double getTotal() {
        return total;
    }

    public int getN() {
        return counts.size();
    }


}
