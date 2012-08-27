package com.thinkaurelius.faunus.formats.rexster;

import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RexsterIterator extends Iterator<JSONObject> {
    public long getItemsIterated();
}
