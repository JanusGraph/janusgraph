package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.exceptions.GraphDatabaseException;
import de.mathnbits.io.DirectoryUtils;
import de.mathnbits.io.Serialization;

import java.io.IOException;
import java.io.Serializable;


public class ObjectDiskStorage {

	protected static final String serializedObjectFileExt = ".obj.dat";
	private static final String temporaryFileExt = ".tmp";
	
	
	private final GraphDatabaseConfiguration config;

	
	public ObjectDiskStorage(GraphDatabaseConfiguration config) {
		this.config=config;
	}

	
	public<O extends Serializable> void putObject(String key,O obj) {
		writeObject(config.getHomePath()+key+serializedObjectFileExt,obj);
	}
	
	public<O extends Serializable> void putObject(String dir, String key,O obj) {
		writeObject(config.getSubdirPath(dir)+key+serializedObjectFileExt,obj);
	}
	
	private<O extends Serializable> void writeObject(String filename, O obj) {
		try {			
			Serialization.writeObjectToFile(filename + temporaryFileExt, obj);
			Serialization.writeObjectToFile(filename, obj);
		} catch (IOException e) {
			throw new GraphDatabaseException("Could not write data to disk.",e);
		}
	}
	
	public<O extends Serializable> O readObject(String filename) {
		try {
			return Serialization.<O>readObjectFromFile(filename);
		} catch (IOException e) {
			//Try to recover from copy
			try {
				O obj = Serialization.<O>readObjectFromFile(filename + temporaryFileExt);
				//Overwrite corrupted copy
				Serialization.writeObjectToFile(filename, obj);
				return obj;
			} catch (IOException e2) {
				throw new GraphDatabaseException("Could not read persisted data from disk.",e);

			}
		}
	}
	
	public<O extends Serializable> O readObject(String filename, O def) {
		try {
			return Serialization.<O>readObjectFromFile(filename);
		} catch (IOException e) {
			//Try to recover from copy
			try {
				O obj = Serialization.<O>readObjectFromFile(filename + temporaryFileExt);
				//Overwrite corrupted copy
				Serialization.writeObjectToFile(filename, obj);
				return obj;
			} catch (IOException e2) {
				return def;
			}
		}
	}
	
	public boolean containsObject(String key) {
		return DirectoryUtils.exists(config.getHomePath()+key+serializedObjectFileExt);
	}
	
	public boolean containsObject(String dir, String key) {
		return DirectoryUtils.exists(config.getSubdirPath(dir)+key+serializedObjectFileExt);
	}
	
	public<O extends Serializable> O getObject(String key) {
	        return (O)readObject(config.getHomePath()+key+serializedObjectFileExt);
	}
	
	public<O extends Serializable> O getObject(String key, O obj) {
		return readObject(config.getHomePath()+key+serializedObjectFileExt,obj);
	}
	
	public<O extends Serializable> O getObject(String dir, String key) {
	        return (O)readObject(config.getSubdirPath(dir)+key+serializedObjectFileExt);
	}

	public<O extends Serializable> O getObject(String dir, String key, O obj) {
		return readObject(config.getSubdirPath(dir)+key+serializedObjectFileExt, obj);
	}
	
	
}
