
package com.thinkaurelius.titan.core;

/**
 * TitanLabel defines the schema for relationships.
 * It provides no extra functionality over {@link TitanType} and is merely the edge type of edges which are relationships.
 * 
 * @see    TitanType
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanLabel extends TitanType {


    public boolean isDirected();


    public boolean isUndirected();


    public boolean isUnidirected();

}
