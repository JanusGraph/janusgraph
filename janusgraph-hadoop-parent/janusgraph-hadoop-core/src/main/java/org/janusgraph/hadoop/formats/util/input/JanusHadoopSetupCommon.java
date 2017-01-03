package org.janusgraph.hadoop.formats.util.input;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class JanusHadoopSetupCommon implements JanusHadoopSetup {

    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    public static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);


    public static final String SETUP_PACKAGE_PREFIX = "org.janusgraph.hadoop.formats.util.input.";
    public static final String SETUP_CLASS_NAME = ".JanusHadoopSetupImpl";

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
