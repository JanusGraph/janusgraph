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
public class TClass2 {

    private final String s;
    private final int i;

    public TClass2(String s, int i) {
        this.s = s;
        this.i = i;
    }

    public String getS() {
        return s;
    }

    public int getI() {
        return i;
    }

    @Override
    public boolean equals(Object oth) {
        TClass2 t = (TClass2)oth;
        return s.equals(t.s) && i==t.i;
    }
}
