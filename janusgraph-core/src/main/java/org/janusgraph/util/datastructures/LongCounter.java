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

package org.janusgraph.util.datastructures;

import java.io.Serializable;

/**
 * A counter with a long value
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LongCounter implements Serializable {

    private static final long serialVersionUID = -880751358315110930L;


    private long count;

    public LongCounter(long initial) {
        count = initial;
    }

    public LongCounter() {
        this(0);
    }

    public void increment(long delta) {
        count += delta;
    }

    public void decrement(long delta) {
        count -= delta;
    }

    public void set(long value) {
        count = value;
    }

    public long get() {
        return count;
    }

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

}
