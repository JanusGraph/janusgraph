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

package org.janusgraph.graphdb.serializer.attributes;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TClass1 {

    private final long a;
    private final float f;

    public TClass1(long a, float f) {
        this.a = a;
        this.f = f;
    }

    public long getA() {
        return a;
    }

    public float getF() {
        return f;
    }

    @Override
    public boolean equals(Object oth) {
        TClass1 t = (TClass1)oth;
        return a==t.a && f==t.f;
    }
}
