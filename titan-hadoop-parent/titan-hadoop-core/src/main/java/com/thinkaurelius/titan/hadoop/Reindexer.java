package com.thinkaurelius.titan.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;

public class Reindexer {

    // Trivial proof-of-concept for launching custom Mappers without any Reducer tasks via HadoopCompiler
    public static void main(String args[]) throws Exception {
        HadoopCompat compat = HadoopCompatLoader.getDefaultCompat();

        // Run a test MR job over Cassandra using a custom Mapper class
        HadoopGraph hg = HadoopFactory.open(args[0]);
        HadoopCompiler compiler = compat.newCompiler(hg);
        Class<? extends Mapper<?,?,?,?>> mapper = InternalReindexMapper.class;
        // Reindex mappers are meant to avoid writing any SequenceFiles,
        // so the KEYOUT and VALUEOUT type parameters are both NullWritable.
        // The Mapper just logs each vertex that it reads.
        compiler.addMap(mapper, NullWritable.class, NullWritable.class, hg.getConf());
        compiler.completeSequence();
        compiler.run(new String[]{});
        hg.shutdown();

        // Run a test MR job over ES shards using EsInputFormat,
        // again using a trivial custom Mapper class.  This Mapper also just
        // logs each document that it reads.
        hg = HadoopFactory.open(args[1]);
        compiler = compat.newCompiler(hg);
        mapper = EsReindexMapper.class;
        compiler.addMap(mapper, NullWritable.class, NullWritable.class, hg.getConf());
        compiler.completeSequence();
        compiler.run(new String[]{});
        hg.shutdown();
    }
}
