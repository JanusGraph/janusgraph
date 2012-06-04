package com.thinkaurelius.titan.util.system;

import java.io.File;

public class IOUtils {

	static public boolean deleteFromDirectory(File path) {
		return deleteDirectory(path,false);
	}

	static public boolean deleteDirectory(File path, boolean includeDir) {
		boolean success = true;
        if( path.exists() ) {
			File[] files = path.listFiles();
			for(int i=0; i<files.length; i++) {
				if(files[i].isDirectory()) {
					success = deleteDirectory(files[i],true) && success;
				}
				else {
					success = files[i].delete() && success;
				}
			}
		}
		if (includeDir)	success = path.delete()  && success;
        return success;
	}

}
