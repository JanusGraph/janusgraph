package com.thinkaurelius.faunus.formats.rexster;

import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;

/**
 * An iterator for JSONObject instances coming back from Rexster.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RexsterIterator extends Iterator<JSONObject> {
    public long getItemsIterated();
}
