// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;

import java.util.ArrayList;
import java.util.Iterator;

public class MemoryEntryList extends ArrayList<Entry> implements EntryList {

    public MemoryEntryList(int size) {
        super(size);
    }

    @Override
    public Iterator<Entry> reuseIterator() {
        return iterator();
    }

    @Override
    public int getByteSize() {
        int size = 48;
        for (Entry e : this) {
            size += 8 + 16 + 8 + 8 + e.length();
        }

        return size;
    }
}
