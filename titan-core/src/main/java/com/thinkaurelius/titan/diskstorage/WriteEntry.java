package com.thinkaurelius.titan.diskstorage;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface WriteEntry extends Entry {

    public<O> O getMetaData(EntryMetaData meta);

}
