package com.thinkaurelius.titan.hadoop.formats.util.input;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SystemTypeInspector {

    public boolean isSystemType(long typeid);

    public boolean isVertexExistsSystemType(long typeid);

    public boolean isVertexLabelSystemType(long typeid);

    public boolean isTypeSystemType(long typeid);

}
