package com.thinkaurelius.faunus.formats.titan.input;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SystemTypeInspector {

    public boolean isSystemType(long typeid);

    public boolean isVertexExistsSystemType(long typeid);

    public boolean isTypeSystemType(long typeid);

}
