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

package org.janusgraph.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;

/**
 * Simple implementation of {@link PartitionAssignment}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimplePartitionAssignment implements PartitionAssignment {

    private int partitionID;

    public SimplePartitionAssignment() {
    }

    public SimplePartitionAssignment(int id) {
        setPartitionID(id);
    }

    public void setPartitionID(int id) {
        Preconditions.checkArgument(id >= 0);
        this.partitionID = id;
    }

    @Override
    public int getPartitionID() {
        return partitionID;
    }
}
