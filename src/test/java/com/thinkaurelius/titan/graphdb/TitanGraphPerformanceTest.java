package com.thinkaurelius.titan.graphdb;


import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.edges.persist.PersistSimpleBinaryTitanEdge;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.testutil.PerformanceTest;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.LinkedHashMap;

public abstract class TitanGraphPerformanceTest extends TitanGraphTestCommon {

	private static final Logger log = LoggerFactory.getLogger(TitanGraphPerformanceTest.class);
	
	private static final int minBatchSizeAsPowerOf2 = 4;
	private static final int maxBatchSizeAsPowerOf2 = 14;
	private final int trials;
	private final int jitPretrials;
	private final boolean tryBatching;

	private final LinkedHashMap<String, Metric> metrics = new LinkedHashMap<String, Metric>();

	public TitanGraphPerformanceTest(Configuration config) {
		this(config, 2, 8, true);
	}
	
	public TitanGraphPerformanceTest(Configuration config, int jitPretrials, int trials, boolean tryBatching) {
		super(config);
		this.jitPretrials = jitPretrials;
		this.trials = trials;
		this.tryBatching = tryBatching;
	}

	@Test
	public void vertexCreation() {
		TitanLabel connect = makeRelationshipType("connect");
		int noNodes = 20000;
		TitanVertex[] nodes = new TitanVertex[noNodes];
		PerformanceTest p = new PerformanceTest(true);
		for (int i=0;i<noNodes;i++) {
			nodes[i]=
				tx.addVertex();
		}
		p.end();
		log.debug("Time per node in (ns): {}", (p.getNanoTime()/noNodes));
		
		p = new PerformanceTest(true);
		for (int i=0;i<noNodes;i++) {
			new PersistSimpleBinaryTitanEdge(connect,(InternalTitanVertex)nodes[i],(InternalTitanVertex)nodes[(i+1)%noNodes]);
		}
		p.end();
		log.debug("Time per edge in (ns): {}", (p.getNanoTime()/noNodes));	
		
		p = new PerformanceTest(true);
		for (int i=0;i<noNodes;i++) {
			nodes[i].addEdge(connect, nodes[(i + 1) % noNodes]);
		}
		p.end();
		log.debug("Time per edge creation+connection in (ns): {}", (p.getNanoTime()/noNodes));	
		tx.abort(); tx=null;
	}
	
	@Test
	public void unlabeledEdgeInsertion() throws Exception {
		runEdgeInsertion(new UnlabeledEdgeInsertion());
	}
	
	@Test
	public void labeledEdgeInsertion() throws Exception {
		runEdgeInsertion(new LabeledEdgeInsertion());
	}
	
	private void runEdgeInsertion(Runnable task) throws Exception {

		if (tryBatching)
			for (int pow = minBatchSizeAsPowerOf2; pow <= maxBatchSizeAsPowerOf2; pow += 1)
				performanceTest(task, true, 1 << pow);
		
		performanceTest(task, false, 0);
	}
	
	private void performanceTest(Runnable task, boolean batching, int batchSize) throws Exception {
		
		String batchStatus;
		if (batching)
			batchStatus = "batching=true; batchSize=" + batchSize;
		else
			batchStatus = "batching=false";
		
		log.info("Beginning {} trials ({} pretrials) of \"{}\"; {}",
				new Object[] { trials, jitPretrials, task, batchStatus });
		
		for (int trial = 0; trial < trials + jitPretrials; trial++) {

			if (jitPretrials == trial)
				metrics.clear();
			
			if (null != config) {
				close();
                config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BATCH_KEY,true);
				open();
			}
			
			if (trial < jitPretrials)
				log.debug("Starting pretrial {}/{}", trial + 1, jitPretrials);
			else
				log.debug("Starting trial {}/{}", trial - jitPretrials + 1, trials);

			task.run();
			
			// Clean database
			tearDown();
			setUp();
			
			// Give the VM a garbage collection timing hint
			System.gc();
			Thread.sleep(100L);
		}
		
		logMetrics();
		log.info("Completed {} trials ({} pretrials) of \"{}\"; {}",
				new Object[] {trials, jitPretrials, task, batchStatus});
	}
	
	private Metric getMetric(String description, String units) {
		String key = description;
		Metric m = metrics.get(key);
		if (null == m) {
			m = new Metric(description, units);
			metrics.put(key, m);
		}
		return m;
	}
	
	private void logMetrics() {
		for (Metric m : metrics.values())
			log.info(m.toString());
	}
	
	private abstract class EdgeInsertion implements Runnable {
		
		protected final int noNodes;
		protected final int noEdgesPerNode;
		
		protected EdgeInsertion(int noNodes, int noEdgesPerNode) {
			this.noNodes = noNodes;
			this.noEdgesPerNode = noEdgesPerNode;
		}
		
		public void run() {

			final long start = System.nanoTime();
			final long mil = 1000000;

			doLoad();
			
			long precommit = System.nanoTime();
			tx.commit();
			long postcommit = System.nanoTime();
			clopen();
			long end = System.nanoTime();

			long precommitMS = (precommit - start) / mil;
			long postcommitMS = (postcommit - start) / mil;
			long endMS = (end - start) / mil;
			
			getMetric("Time from start to just before commit", "ms").addValue(
					precommitMS);
			getMetric("Time from start through commit", "ms").addValue(
					postcommitMS);
			getMetric("Total time", "ms").addValue(endMS);

			long edgesPerSec = noNodes * noEdgesPerNode * 1000
					/ Math.max(1, postcommitMS - precommitMS);
			getMetric("TitanRelation commit rate", "edges/sec").addValue(edgesPerSec);
		}
		
		protected abstract void doLoad();
	}
	
	private class LabeledEdgeInsertion extends EdgeInsertion {
		
		private LabeledEdgeInsertion() {
			super(50000, 10);
		}

		@Override
		public String toString() {
			return "labeled edge insertion";
		}
		
		@Override
		protected void doLoad() {
			TitanKey weight = makeWeightPropertyType("weight");
			TitanKey id = makeIDPropertyType("id");
			TitanLabel knows = makeLabeledRelationshipType("knows",id,weight);
			TitanKey name = makeStringPropertyType("name");
			
			String[] names = new String[noNodes];
			TitanVertex[] nodes = new TitanVertex[noNodes];
			for (int i=0;i<noNodes;i++) {
				do {
					names[i]=RandomGenerator.randomString();
					// Retry in case of collision with existing name
				} while (null != tx.getVertex(name, names[i]));
				nodes[i] = tx.addVertex();
				nodes[i].addProperty(name, names[i]);
			}
			int offsets[] = {-99, -71, -20, -17, -13, 2, 7, 15, 33, 89};
			assert offsets.length==noEdgesPerNode;
			
			for (int i=0;i<noNodes;i++) {
				TitanVertex n = nodes[i];
				for (int e=0;e<noEdgesPerNode;e++) {
					TitanVertex n2 = nodes[wrapAround(i+offsets[e],noNodes)];
					TitanEdge r = n.addEdge(knows, n2);
					r.addProperty(id, RandomGenerator.randomInt(0, Integer.MAX_VALUE));
					r.addProperty(weight, Math.random());
				}
				if (i%10000==0) log.debug(""+i);
			}
		}
	}
	
	private class UnlabeledEdgeInsertion extends EdgeInsertion {
		
		public UnlabeledEdgeInsertion() {
			super(100000, 10);
		}
		
		@Override
		public String toString() {
			return "unlabeled edge insertion";
		}

		@Override
		public void doLoad() {

			TitanLabel connect = makeRelationshipType("connect");
			TitanKey name = makeStringPropertyType("name");
			
			String[] names = new String[noNodes];
			TitanVertex[] nodes = new TitanVertex[noNodes];
			for (int i = 0; i < noNodes; i++) {
				do {
					names[i]= RandomGenerator.randomString();
					// Retry in case of collision with existing name
				} while (null != tx.getVertex(name, names[i]));
				nodes[i] = tx.addVertex();
				nodes[i].addProperty(name, names[i]);
			}
			int offsets[] = { -99, -71, -20, -17, -13, 2, 7, 15, 33, 89 };
			assert offsets.length == noEdgesPerNode;

			for (int i = 0; i < noNodes; i++) {
				TitanVertex n = nodes[i];
				for (int e = 0; e < noEdgesPerNode; e++) {
					TitanVertex n2 = nodes[wrapAround(i + offsets[e], noNodes)];
					n.addEdge(connect, n2);
				}
				if ((i + 1) % 10000 == 0)
					log.debug("" + i);
			}
		}
	}


    private static class Metric {
        private final String description;
        private final String units;
        private final DescriptiveStatistics stats;

        Metric(String description, String units) {
            this.description = description;
            this.units = units;
            this.stats = new DescriptiveStatistics();
        }

        void addValue(double d) {
            stats.addValue(d);
        }

        @Override
        public String toString() {
            NumberFormat fmt = NumberFormat.getNumberInstance();
            fmt.setMaximumFractionDigits(1);

            return  description + ":" +
                    " avg=" + fmt.format(stats.getMean()) +
                    " stdev=" + fmt.format(stats.getStandardDeviation()) +
                    " " + units +
                    " (" + stats.getN() + " samples)";
        }
    }

}


