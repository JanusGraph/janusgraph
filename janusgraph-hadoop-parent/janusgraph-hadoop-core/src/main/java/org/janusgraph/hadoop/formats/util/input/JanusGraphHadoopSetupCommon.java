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

package org.janusgraph.hadoop.formats.util.input;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class JanusGraphHadoopSetupCommon implements JanusGraphHadoopSetup {

    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    public static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);


    public static final String SETUP_PACKAGE_PREFIX = "org.janusgraph.hadoop.formats.util.input.";
    public static final String SETUP_CLASS_NAME = ".JanusGraphHadoopSetupImpl";

    @Override
    public SliceQuery inputSlice() {
        //For now, only return the full range because the current input format needs to read the hidden
        //vertex-state property to determine if the vertex is a ghost. If we filter, that relation would fall out as well.
        return DEFAULT_SLICE_QUERY;
    }

    @Override
    public void close() {
        //Do nothing
    }

}
