// Copyright 2022 Unified Catalog Team
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

package org.janusgraph.graphdb.database.index;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class IndexUpdateContainer {

    private Set<IndexUpdate> addDelete = null;

    private IndexUpdate updateOnly = null;

    public IndexUpdateContainer(IndexUpdate indexUpdate) {
        if (indexUpdate.isUpdate()) {
            updateOnly = indexUpdate;
        } else {
            initSet(indexUpdate);
        }
    }

    public void add(IndexUpdate indexUpdate) {
        if (!indexUpdate.isUpdate()) {
            initSet(indexUpdate);
        }
    }

    public Stream<IndexUpdate> getUpdates() {
        if (updateOnly != null) {
            return Stream.of(updateOnly);
        } else {
            return this.addDelete.stream();
        }
    }

    private void initSet(IndexUpdate indexUpdate) {
        if (this.addDelete == null) {
            this.addDelete = new HashSet<>();
        }

        this.addDelete.add(indexUpdate);
        updateOnly = null;
    }

}
