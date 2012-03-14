package com.thinkaurelius.titan.util.test;

import java.io.File;

public class IOUtils {

	static public void deleteFromDirectory(File path) {
		deleteDirectory(path,false);
	}

	static public void deleteDirectory(File path, boolean includeDir) {
		if( path.exists() ) {
			File[] files = path.listFiles();
			for(int i=0; i<files.length; i++) {
				if(files[i].isDirectory()) {
					deleteDirectory(files[i],true);
				}
				else {
					files[i].delete();
				}
			}
		}
		if (includeDir)	path.delete();
	}

}
