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

package org.janusgraph.graphdb.database.idassigner;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StaticIDBlockSizer implements IDBlockSizer {

    private final long blockSize;
    private final long blockSizeLimit;

    public StaticIDBlockSizer(long blockSize, long blockSizeLimit) {
        Preconditions.checkArgument(blockSize > 0);
        Preconditions.checkArgument(blockSizeLimit > 0);
        Preconditions.checkArgument(blockSizeLimit > blockSize,"%s vs %s",blockSizeLimit,blockSize);
        this.blockSize = blockSize;
        this.blockSizeLimit = blockSizeLimit;
    }

    @Override
    public long getBlockSize(int idNamespace) {
        return blockSize;
    }

    @Override
    public long getIdUpperBound(int idNamespace) {
        return blockSizeLimit;
    }
}
