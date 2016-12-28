package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum HashUtility {

    SHORT {
        @Override
        public HashFunction get() {
            return Hashing.murmur3_32();
        }
    },

    LONG {
        @Override
        public HashFunction get() {
            return Hashing.murmur3_128();
        }
    };


    public abstract HashFunction get();


}
