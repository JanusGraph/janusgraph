package com.thinkaurelius.titan.diskstorage.util;

import cern.colt.map.AbstractIntIntMap;
import cern.colt.map.OpenIntIntHashMap;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.exceptions.GraphDatabaseException;
import de.mathnbits.io.Serialization;

import java.io.File;
import java.io.IOException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class LocalIDManager {
    
    public static final String DEFAULT_NAME = "idmanager.dat";

    private static final String temporaryFileExt = ".tmp";
    
    private final AbstractIntIntMap idmap;
    private final String filename;
    
    public LocalIDManager(String file) {
        Preconditions.checkNotNull(file);
        this.filename = file;
        File f = new File(filename);
        if (f.exists()) {
            Preconditions.checkArgument(f.isFile(),"Need to specify a filename for id map");
            Preconditions.checkArgument(f.canRead() && f.canWrite(),"Need to be able to access file for id map");
            try {
                idmap = Serialization.readObjectFromFile(filename);
            } catch (IOException e) {
                throw new IllegalArgumentException("Could not read id map from file: " + filename,e);
            }
        } else {
            idmap = new OpenIntIntHashMap();   
        }
    }

    public synchronized long[] getIDBlock(int partition, int blockSize) {
        int current = idmap.get(partition);
        Preconditions.checkArgument(Integer.MAX_VALUE-blockSize>current,"ID overflow for partition: " + partition);
        int next = current + blockSize;
        idmap.put(partition,next);
        save();
        return new long[]{current,next};
    }
    
    
    private void save() {
        try {
            Serialization.writeObjectToFile(filename + temporaryFileExt, idmap);
            Serialization.writeObjectToFile(filename, idmap);
        } catch (IOException e) {
            throw new GraphDatabaseException("Could not write id map to disk for file: " + filename,e);
        }
    }

}
