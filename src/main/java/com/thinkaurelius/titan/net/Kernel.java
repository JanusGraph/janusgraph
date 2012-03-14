package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;
import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Message;
import com.thinkaurelius.titan.net.msg.Query;
import com.thinkaurelius.titan.net.msg.codec.MessageCodec;
import edu.umd.umiacs.tcpcom.EventHandler;
import edu.umd.umiacs.tcpcom.MiniBBOutput;
import edu.umd.umiacs.tcpcom.TcpSocketCommunicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The core network messaging class.
 *
 * @author dalaro
 */
public class Kernel implements CommunicationFramework {

    //
    // Final static variables
    //

    private static final Logger log = LoggerFactory.getLogger(Kernel.class);
    
    private static final int defaultListenPort = 36462;

    //
    // Final variables
    //

    private final TcpSocketCommunicator com;
    private final InetSocketAddress     listen;  

    /** Accounting objects for queries created by this node */
    private final ConcurrentHashMap<Key, SeedTracker> queryTrackers =
            new ConcurrentHashMap<Key, SeedTracker>();
    
    /** Executor for all tasks */
    private final LoggingScheduledThreadPoolExecutor executor;
    
    /**
     * Used for estimating overall machine load
     * and ultimately rejecting incoming queries if the
     * load is over a configurable limit.  Also used
     * to determine the machine's processing core count.
     */
    private final OperatingSystemMXBean osMXBean =
        ManagementFactory.getOperatingSystemMXBean();

    /**
     * All queries the system is trying to answer.
     * Includes both queries awaiting  processing and
     * queries currently being processed by some thread.
     */
    private final ConcurrentHashMap<Key, InstanceWorklog> queryWorklogs =
            new ConcurrentHashMap<Key, InstanceWorklog>();

    /**  Stored procedures indexed by query type */
    private final ConcurrentHashMap<Integer, QueryType<?,?>> idToQueryType =
    	new ConcurrentHashMap<Integer, QueryType<?, ?>>();
    private final ConcurrentHashMap<Class<? extends QueryType<?,?>>, Integer> 
    	queryTypeClassToId =
    	new ConcurrentHashMap<Class<? extends QueryType<?,?>>, Integer>();
    

    /**
     * Seeds of killed queries.  Any attempt to submit these for processing
     * will be refused.
     */
    private final ConcurrentSkipListSet<Key> killedQueries =
            new ConcurrentSkipListSet<Key>();
    
    /**
     * Maps instance keys of Queries the system is currently trying to forward
     * to remote hosts for further processing to their sender objects.
     */
    private final Map<Key, IndividualQuerySender> forwardedQueriesOutstanding =
    	new ConcurrentHashMap<Key, IndividualQuerySender>();
    
    /**
     * Used to generate node-unique ids for message identification.
     */
    private final AtomicLong ids = new AtomicLong();
    
    /**
     * Graph database handle.  Entry point for all node/vertex operations
     * and critical for starting "transactions" in which to execute queries.
     */
    private final GraphDB gdb;
    
    /**
     * Serializer (and Deserializer) for Titan types.  In particular it
     * can deserialize/serialize QueryType.queryType() and
     * QueryType.resultType().
     */
    private final Serializer serializer;
    
    private final EventHandler eventHandler;
    
    /**
     * Mapper from Titan node-ids (type long) to arrays of Titan node
     * InetSocketAddresses the mapper thinks should have a copy of that
     * node replicated locally.  Used for query forwarding and sending.
     */
    private final NodeID2InetMapper node2inet;

    /**
     * Maximum number of running and waiting-to-run queries.
     * When the system has at least this many queries,
     * it rejects incoming queries with a "too busy" message.
     */
    private final int maxQueryCount = 2048;

    /**
     * Maximum operating system load divided by available processor count,
     * as reported by the standard library OperatingSystemMXBean.
     * When the system has at least this per-cpu load, it rejects incoming
     * queries with a "too busy" message.
     */
    private final double maxLoadPerCpu = 5.00;
    
    /** 
     * Completely non-empirical guess at the Serializer's DataOutput
     * initial buffer size.  This exact value has no merit.  But some
     * value must be provided to the DataOutput object to even
     * instantiate it.
     */
    private final int queryLoadSerializationGuess = 128;
    
    //
    // Nonfinal Variables
    //
    
    /**
     * Whether the kernel is {@link #start()}ed.
     */
    private boolean started;

    /**
     * The value of {@link System#currentTimeMillis() when
     * this Kernel was {@link #start()}ed.
     */
    private long bootTime;
    
    /**
     * 
     * @throws java.io.IOException on failure to bind to {@code listen}
     * @throws RuntimeException wrapping NoSuchMethodException or SecurityException if
     *         reflection-based internal setup methods fail
     */
    public Kernel(InetSocketAddress listen, GraphDB gdb, Serializer serializer,
    		NodeID2InetMapper node2inet) throws IOException {
    	this.listen = listen;
    	this.eventHandler = new SocketEventHandler(this);
        this.com = new TcpSocketCommunicator(listen, eventHandler);
        this.gdb = gdb;
        this.serializer = serializer;
        this.node2inet = node2inet;
                
        /*
         * This executor extends the standard ScheduledThreadPoolExecutor.
         * 
         * As noted in the Javadoc for ScheduledThreadPool executor, this
         * object internally uses a fixed-size thread pool with the number
         * of threads equal to the corePoolSize constructor argument.
         * 
         * Therefore the setMaximumPoolSize(int) method inherited from
         * ThreadPoolExecutor has no effect, as written in the same Javadoc.
         * 
         * To modify the number of threads used by this executor,
         * change the threadCount variable set immediately below.
         */
        int threadCount = osMXBean.getAvailableProcessors();
        this.executor = new LoggingScheduledThreadPoolExecutor(threadCount);
    }
    
    public Kernel(GraphDB gdb, Serializer serializer,
    		NodeID2InetMapper node2inet) throws IOException {
        this(getDefaultListen(), gdb, serializer, node2inet);
    }
    
    private static InetSocketAddress getDefaultListen() {
    	try {
    		return new InetSocketAddress(
    			InetAddress.getLocalHost(), getDefaultListenPort());
    	} catch (UnknownHostException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    /* TODO many of these methods are marked "Internal" despite public access
     * 
     * Most (maybe not all) of these methods are used by Handler classes
     * outside this package, so they have public access.  As the Handler
     * interface firms up, the "Internal" methods should become an
     * interface like e.g. KernelInternal. 
     */
    
    /**
     * Internal
     * 
     * @return Generate a new globally unique identifier {@link Key} object
     */
    public Key generateKey() {
    	return new Key(ids.getAndIncrement(), listen, bootTime);
    }

    /**
     * Internal
     *
     * @throws IllegalArgumentException if {@code e} does not have destination
     * addresses set
     */
    public <T extends Message> T send(T m, InetSocketAddress to) {
        if (null == to) {
            String err = "Attempted to send message without destination: " + m;
            throw new IllegalArgumentException(err);
        }

        m.setReplyPort(listen.getPort());
        MiniBBOutput out = new MiniBBOutput();
        MessageCodec.getDefaultCodec().encode(m, out);
        com.send(out.getBuffers(), to);
        log.debug("[" + listen + "] Sent " + m + " to " + to);
        return m;
    }

    /**
     * Internal
     * 
     * Estimates load on the local machine using
     * {@link java.lang.management.OperatingSystemMXBean}
     * and the hard maximum query count, {@code maxQueryCount}.
     *
     * @return true if this machine thinks it can handle additional queries,
     * false if it thinks it cannot handle additional queries.
     */
    public synchronized boolean isQueryCapacityFree() {
        final double loadPerCpu = osMXBean.getSystemLoadAverage() /
                osMXBean.getAvailableProcessors();
        
        final int queryCount;
        synchronized (queryWorklogs) {
        	queryCount = queryWorklogs.size();
        }

        return maxQueryCount > queryCount && maxLoadPerCpu > loadPerCpu;
    }    

    /**
     * Internal
     * 
     * Get a query type (aka "stored procedure")
     * 
     * @param id integer QueryType id for internal use by Kernel
     * @return QueryType object passed to {@link #registerQueryType(com.thinkaurelius.titan.core.query.QueryType)}
     */
    public QueryType<?,?> getProc(int id) {
        return idToQueryType.get(id);
    }

    /**
     * Internal
     * 
     * Execute a Runnable immediately on the Kernel's main thread pool
     * 
     * @param r Runnable to execute
     */
    public void executeNow(Runnable r) {
    	executor.submit(r);
    }
    
    /**
     * Internal
     * 
     * Execute a Runnable after a specified time delay on the
     * Kernel's main thread pool
     * 
     * @param r Runnable to execute
     * @param delay Execution time delay amount
     * @param unit Execution time delay unit
     */
    public ScheduledFuture<?> scheduleExecution(Runnable r, long delay, TimeUnit unit) {
    	return executor.schedule(r, delay, unit);
    }

    /**
     * Internal
     * 
     * Return the Titan-internal Serializer implementation
     * 
     * @return Serializer object passed to this Kernel's constructor
     */
    public Serializer getSerializer() {
    	return serializer;
    }
    
    /**
     * Internal
     * 
     * Get the InetSocketAddress on which this Kernel is
     * listening for new connections from other Kernels
     * 
     * @return InetSocketAddress that accepts remote connections
     */
    public InetSocketAddress getListenAddress() {
    	return listen;
    }
    
    public static final int getDefaultListenPort() {
    	return defaultListenPort;
    }
    
    public long getBootTime() {
    	return bootTime;
    }
    
    /**
     * Internal
     * 
     * Return the {@link NodeID2InetMapper} implementation used internally
     * by this Kernel when answering queries.
     * 
     * @return node-to-host mapper
     */
    public NodeID2InetMapper getNodeID2InetMapper() {
    	return node2inet;
    }
    
    // End internal methods
    
    /**
     * Begin processing messages.
     *
     * @throws IllegalStateException when already started
     */
    public void start() {
        if (started)
            throw new IllegalStateException("Already started");

        bootTime = System.currentTimeMillis();
        
        com.start();
        
        started = true;
    }

     /**
     * Stop handling messages.
     *
     * @param millis maximum milliseconds to wait for clean shutdown.
     * @return whether the shutdown succeeded in {@code millis} milliseconds.
     * True indicates successful shutdown, false indicates otherwise.
     * A successful shutdown is one in which all threads are interrupted,
     * joined, and return false from {@code isAlive()} before the {@code millis}
     * time limit runs out.  If this method returns false, the messager may
     * still contain running threads.  If it returns true, all threads are dead
     * and no threads will be started until the next invocation of
     * {@link #start()}
     * @throws InterruptedException
     */
    public boolean shutdown(long millis) throws InterruptedException {
        if (!started)
            throw new IllegalStateException("Not started");

        long begin = System.currentTimeMillis();
        long timeleft = millis - (System.currentTimeMillis() - begin);
        
        com.shutdown();
        
        timeleft = millis - (System.currentTimeMillis() - begin);
        if (0 >= timeleft)
        	return false;

        executor.shutdown();
        boolean success = 
        	executor.awaitTermination(timeleft, TimeUnit.MILLISECONDS);
        
        started = false;
        return success;
    }
    
    /*
     * TODO: all the query-state related methods here, along with the
     * fields on Kernel they manipulate, probably better belong factored
     * out into either the Query object itself or an object dedicated
     * to tracking query state, or some combination of the two.
     * 
     * Among the current problems: there is no notion of timestamping on
     * the state information thrown ad-hoc into Kernel fields, so references
     * may be "lost" in a map by programming error (that is, inserted and
     * never removed, even after they become useless), a classic memory leak.
     * Structurally, this state doesn't really seem to belong.  The rest of
     * the Kernel methods deal with opaque abstractions, like Serializer,
     * Executor, Runnable, NodeID2InetMapper, etc, but this doesn't even deal
     * with the Query object as an abstraction -- it deals most often with
     * objects of type Key that identify Query objects.  Finally, these
     * methods are really used to facilitate state transitions in a state
     * machine covering query execution whose implementation is spread out
     * over all the little -Handler classes in e.u.u.titan.net.handler.
     * This code would likely be more approachable if the "guts" of the
     * query state machine -- states and events/transitions  -- were all in
     * one or two classes.
     */
    
    /**
     * Check whether a query seed is marked killed.
     *
     * @param seed query seed to check
     * @return true if killed, false otherwise
     */
    public boolean isSeedKilled(Key seed) {
        return killedQueries.contains(seed);
    }

    /**
     * Mark a query seed as killed.
     *
     * Only changes this object's internal state.  Does not send any
     * messages to other nodes.
     *
     * @param seed query seed to kill
     */
    public void killSeed(Key seed) {
        killedQueries.add(seed);
    }

    public void registerQuery(Query q) {
    	InstanceWorklog wl;
        wl = new InstanceWorklog(q);
	    wl.arrived(System.currentTimeMillis());
    	synchronized (queryWorklogs) {
    	    queryWorklogs.put(q.getInstance(), wl);
    	}
    }

    public void holdQuery(Key instance) {
    	InstanceWorklog wl;
    	synchronized (queryWorklogs) {
    		wl = queryWorklogs.get(instance);
    	}
    	wl.held(System.currentTimeMillis());
    	log.debug("Held " + instance);
    }

    public Query unholdQuery(Key instance) {
    	InstanceWorklog wl;
    	synchronized (queryWorklogs) {
	        wl = queryWorklogs.get(instance);
    	}
        if (wl == null)
            return null;
        wl.unheld(System.currentTimeMillis());
        log.debug("Unheld " + instance);
        return wl.getQuery();
    }

    public void runqueueQuery(Key instance) {
    	InstanceWorklog wl;
    	synchronized (queryWorklogs) {
    		wl = queryWorklogs.get(instance);
    	}
        wl.runqueued(System.currentTimeMillis());
        // Enqueue query for execution
        executor.submit(new QueryRunnable(this, wl.getQuery(), gdb));
        log.debug("Enqueued " + wl.getQuery());
    }
    
    public void startQuery(Key instance) {
    	synchronized (queryWorklogs) {
    		queryWorklogs.get(instance).started(System.currentTimeMillis());
    	}
    }

    public void finishQuery(Key instance) {
    	synchronized (queryWorklogs) {
    		InstanceWorklog wl = queryWorklogs.get(instance);
    		wl.finished(System.currentTimeMillis());
    		queryWorklogs.remove(instance);
    	}
    }

    /**
     * Immediately enqueue the provided {@link com.thinkaurelius.titan.net.IndividualQuerySender}
     * for execution on the Kernel's main executor.  Also, record
     * the {@code IndividualQuerySender} in the Kernel'
     * {@link #forwardedQueriesOutstanding} Map.
     * 
     * @param s
     */
    public void sendQuery(IndividualQuerySender s) {
    	forwardedQueriesOutstanding.put(s.getInstance(), s);
    	executeNow(s);
    }

    /**
     * Cancels any outstanding {@link QTimeout} earlier registered with
     * {@link #registerQueryTimeout()}.
     *
     * @param instance Identifying key of the query instance
     * @return true if the timeout was found and canceled, false otherwise
     */
    public IndividualQuerySender cancelForwardingTimeout(Key instance) {
    	IndividualQuerySender thand = forwardedQueriesOutstanding.remove(instance);
        if (null != thand && thand.cancelFuture()) {
        	return thand;	
        }
        return null;
    }
    
    /**
     * Sends a brand new query that is just being created/seeded on the cluster.
     * 
     * @param nodeid
     * @param queryLoad
     * @param queryTypeClass
     * @param resultCollector
     */
    public void sendQuery(long nodeid, Object queryLoad,
					Class<? extends QueryType<?,?>> queryTypeClass,
					ResultCollector<?> resultCollector) {
    	// Map node2inet's InetAddress list into an InetSocketAddress list
    	List<InetAddress> hosts =
    		Arrays.asList(node2inet.getInetAddress(nodeid));
    	final int port = getListenAddress().getPort();
    	List<InetSocketAddress> peers =
    		new ArrayList<InetSocketAddress>(hosts.size());
    	for (InetAddress a : hosts) {
    		peers.add(new InetSocketAddress(a, port));
    	}
    	
    	DataOutput loadWriter =
    		serializer.getDataOutput(queryLoadSerializationGuess, true);
    	loadWriter.writeObjectNotNull(queryLoad);
    	ByteBuffer load[] = new ByteBuffer[]{loadWriter.getByteBuffer()};
    	Integer type = queryTypeClassToId.get(queryTypeClass);
    	if (null == type) {
    		throw new IllegalArgumentException("Unknown query type class: " + 
    				queryTypeClass);
    	}
    	Key seed = generateKey();
    	
    	QueryType<?,?> qt = idToQueryType.get(type);
    	
    	Query query = new Query(seed, type, nodeid, load);
    	seedQuery(query, resultCollector, qt.resultType());
    	sendQuery(new IndividualQuerySender(this, query, peers));
    }

    public Collection<InstanceWorklog> getQueryWorklogs() {
        return queryWorklogs.values();
    }

    public SeedTracker getQueryTracker(Key seed) {
        SeedTracker result = queryTrackers.get(seed);
//        if (null == result) {
//                queryTrackers.putIfAbsent(seed, new SeedTracker(q));
//                result = queryTrackers.get(seed);
//        }
//        assert null != result;
        return result;
    }
    
    /**
     * Called by the local system to start a query on the cluster.
     * 
     * @param q Query to launch
     * @return tracker object for the query
     */
    public SeedTracker seedQuery(Query q, ResultCollector<?> resultCollector, Class<?> resultClass) {
    	SeedTracker t = new SeedTracker(this, q, resultCollector, resultClass);
    	return queryTrackers.put(q.getSeed(), t);
    }
    
    // End query-related methods

	@Override
	public <T, U> void registerQueryType(QueryType<T, U> queryType) {
		int index = idToQueryType.keySet().size();
		queryTypeClassToId.put((Class<? extends QueryType<?, ?>>) queryType.getClass(), index);
		idToQueryType.put(index, queryType);
	}

	@Override
	public QuerySender createQuerySender() {
		return new ClientQuerySender(this);
	}
}