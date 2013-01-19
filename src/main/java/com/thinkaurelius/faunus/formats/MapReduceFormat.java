package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MapReduceFormat {

    public void addMapReduceJobs(final FaunusCompiler compiler) throws IOException;
}
