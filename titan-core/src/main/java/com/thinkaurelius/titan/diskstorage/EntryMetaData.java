package com.thinkaurelius.titan.diskstorage;

import java.util.EnumMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum EntryMetaData {

    TTL, VISIBILITY;


    public static byte[] serializeMetaData(EnumMap<EntryMetaData,Object> metadata) {
        return new byte[0]; //TODO: implement
    }

}
