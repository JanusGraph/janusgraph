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

package org.janusgraph.diskstorage;

import java.util.Objects;

public class KeyColumn {

    public final int key;
    public final int column;

    public KeyColumn(int key, int column) {
        this.key = key;
        this.column = column;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, column);
    }

    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!getClass().isInstance(other)) return false;
        KeyColumn oth = (KeyColumn) other;
        return key == oth.key && column == oth.column;
    }

}
