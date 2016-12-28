package com.thinkaurelius.titan.core.log;

/**
 * Identifies the type of change has undergone. Used in {@link ChangeState} to retrieve those elements
 * that have been changed in a certain way.
 * <p/>
 * {@link #ADDED} applies to elements that have been added to the graph, {@link #REMOVED} is for removed elements, and
 * {@link #ANY} is used to retrieve all elements that have undergone change.
 * <p/>
 * {@link #ADDED} and {@link #REMOVED} are considered proper change states.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum Change {

    ADDED, REMOVED, ANY;

    public boolean isProper() {
        switch(this) {
            case ADDED:
            case REMOVED:
                return true;
            default:
                return false;
        }
    }

}
