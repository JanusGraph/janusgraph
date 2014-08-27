package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import com.thinkaurelius.titan.hadoop.HadoopPipeline;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.hdfs.HDFSTools;
import com.thinkaurelius.titan.hadoop.hdfs.TextFileLineIterator;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.ToStringPipe;
import com.tinkerpop.pipes.util.iterators.SingleIterator;

import groovy.lang.Closure;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.groovy.tools.shell.IO;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ResultHookClosure extends Closure {
    private final String resultPrompt;
    private final IO io;
    private static final int LINES = 15;

    public ResultHookClosure(final Object owner, final IO io, final String resultPrompt) {
        super(owner);
        this.io = io;
        this.resultPrompt = resultPrompt;
    }

    public Object call(final Object[] args) {
        final Object result = args[0];
        final Iterator itty;
        if (result instanceof HadoopPipeline) {
            try {
                final HadoopPipeline pipeline = (HadoopPipeline) result;
                pipeline.submit();
                final FileSystem hdfs = FileSystem.get(pipeline.getGraph().getConf());
                final Path output = HDFSTools.getOutputsFinalJob(hdfs, pipeline.getGraph().getJobDir().toString());
                itty = new TextFileLineIterator(hdfs, hdfs.globStatus(new Path(output.toString() + "/" + Tokens.SIDEEFFECT + "*")), LINES);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            itty = new ToStringPipe();
            ((Pipe) itty).setStarts(new SingleIterator<Object>(result));
        }

        int counter = 0;
        while (itty.hasNext()) {
            counter++;
            this.io.out.println(this.resultPrompt + itty.next());
        }
        if (counter == LINES)
            this.io.out.println(this.resultPrompt + "...");

        return null;
    }
}