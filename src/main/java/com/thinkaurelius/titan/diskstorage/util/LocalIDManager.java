package com.thinkaurelius.titan.diskstorage.util;

import cern.colt.map.AbstractIntIntMap;
import cern.colt.map.OpenIntIntHashMap;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.GraphDatabaseException;
import com.thinkaurelius.titan.core.GraphStorageException;

import java.io.*;

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
                idmap = readObjectFromFile(filename);
            } catch (IOException e) {
                throw new IllegalArgumentException("Could not read id map from file: " + filename,e);
            }
        } else {
            idmap = new OpenIntIntHashMap();   
        }
    }

    public synchronized long[] getIDBlock(int partition, int blockSize) {
        int current = idmap.get(partition);
        if (current==0) current=1; //starting point
        Preconditions.checkArgument(Integer.MAX_VALUE-blockSize>current,"ID overflow for partition: " + partition);
        int next = current + blockSize;
        idmap.put(partition,next);
        save();
        return new long[]{current,next};
    }
    
    
    private void save() {
        try {
            writeObjectToFile(filename + temporaryFileExt, idmap);
            writeObjectToFile(filename, idmap);
        } catch (IOException e) {
            throw new GraphStorageException("Could not write id map to disk for file: " + filename,e);
        }
    }


    @SuppressWarnings("unchecked")
    public static final<T> T readObjectFromFile(String filename) throws IOException {
        FileInputStream f = new FileInputStream(filename);
        ObjectInputStream objstream = new ObjectInputStream(f);
        Object ret = null;
        try {
            ret = objstream.readObject();
        } catch (ClassNotFoundException e) {
            throw new GraphStorageException("Could not find class of read object");
        } finally {
            f.close();
        }
        if (ret == null) throw new GraphStorageException("Could not read any object from file");
        return (T)ret;
    }

    public static final<T extends Serializable> void writeObjectToFile(String filename, T obj) throws IOException {
        FileOutputStream f = new FileOutputStream(filename);
        ObjectOutputStream objstream = new ObjectOutputStream (f);
        objstream.writeObject ( obj );
        f.close();
    }

}
