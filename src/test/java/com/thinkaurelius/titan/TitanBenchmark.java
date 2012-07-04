package com.thinkaurelius.titan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import org.apache.commons.lang.time.StopWatch;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanBenchmark {
    
    
    public static void main(String[] args) throws IOException {
        Preconditions.checkArgument(args.length==3);
        final int batchSize = Integer.parseInt(args[2]);
        final int maxTime = 1000000;

        String configFile = args[0];
        String dataFile = args[1];

        TitanGraph g = TitanFactory.open(configFile);

        boolean skipLoading = false;
        TitanTransaction tx = g.startTransaction();
        if (tx.containsType("pid")) skipLoading=true;
        tx.stopTransaction(TransactionalGraph.Conclusion.FAILURE);

        StopWatch watch = new StopWatch();
        if (!skipLoading) {
            tx = g.startTransaction();
            TitanKey vertexid = tx.makeType().name("pid").dataType(Integer.class).
                    functional(false).indexed().unique().makePropertyKey();
            TitanKey time = tx.makeType().name("time").dataType(Integer.class).
                    functional(false).makePropertyKey();
            TitanLabel follows = tx.makeType().name("follows").primaryKey(time).makeEdgeLabel();
            tx.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);

            BatchGraph loader = new BatchGraph(g, BatchGraph.IdType.NUMBER, batchSize);
            loader.setEdgeIdKey(null);
            loader.setVertexIdKey("pid");
            Scanner data = new Scanner(new File(dataFile));
            Random random = new Random();
            watch.start();
            try {
                int loaded=0;
                while (data.hasNext()) {
                    String line = data.nextLine().trim();
                    if (!line.isEmpty()) {
                        String[] eline = line.split("\\t");
                        Preconditions.checkArgument(eline.length==2,line);
                        Vertex[] vs = new Vertex[2];
                        for (int i=0;i<2;i++) {
                            int id = Integer.parseInt(eline[i]);
                            vs[i]=loader.getVertex(id);
                            if (vs[i]==null) vs[i]=loader.addVertex(id);
                        }
                        Edge e = loader.addEdge(null,vs[0],vs[1],"follows");
                        e.setProperty("time",random.nextInt(maxTime)+1);
                    }
                    loaded++;
                    if (loaded%10000==0) System.out.println("Loaded: " + loaded);
                }
            } finally {
                data.close();
            }
            loader.shutdown();
            watch.stop();
            System.out.println("Loading took (sec): " + watch.getTime()/1000);

            g = TitanFactory.open(configFile);
        }

        tx = g.startTransaction();
        try {
            Vertex v = tx.getVertex("pid", 12);
            Preconditions.checkNotNull(v);
            watch.reset();watch.start();
            System.out.println("Size Q1: "+v.query().labels("follows").direction(Direction.OUT).interval("time",0,1000).count());
            watch.stop();
            System.out.println("Query 1 (ms): " + watch.getTime());


            watch.reset();watch.start();
            System.out.println("Size Q2: "+v.query().labels("follows").direction(Direction.OUT).count());
            watch.stop();
            System.out.println("Query 2 (ms): " + watch.getTime());
        } finally {
            tx.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            g.shutdown();
        }

    }
        
}
