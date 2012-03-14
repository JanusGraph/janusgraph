package com.thinkaurelius.titan.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class LoggingScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
	private static final Logger log =
		LoggerFactory.getLogger(LoggingScheduledThreadPoolExecutor.class);
	
	private final AtomicLong errors = new AtomicLong();
	private final AtomicLong completed = new AtomicLong();
	
	public LoggingScheduledThreadPoolExecutor(int corePoolSize) {
		super(corePoolSize);
	}

	@Override
	protected <V> RunnableScheduledFuture<V> decorateTask(Runnable r,
			RunnableScheduledFuture<V> task) {
		return new ExceptionLoggingTask<V>(r, task);
	}

	@Override
	protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> c,
			RunnableScheduledFuture<V> task) {
		return new ExceptionLoggingTask<V>(c, task);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		
		if (null != t) {
			/*
			 * We should never get to this code, even when a thread
			 * throws an exception.  Because ThreadPoolExecutor and
			 * ScheduledThreadPoolExecutor enclose all tasks in FutureTask
			 * and derived classes, and because FutureTask handles
			 * exceptions internally with the setException() method and
			 * does not present a Throwable in this method, this
			 * codepath should never be reached.  See the Javadoc for
			 * ThreadPoolExecutor and ScheduledThreadPoolExecutor for
			 * proof of these claims about Exception handling.
			 */
			log.warn("This method should not be called in normal operation.");
			log.warn("Abnormal thread termination", t);
			log.warn("Terminated Runnable: " + r);
			errors.incrementAndGet();
		} else {
			completed.incrementAndGet();
		}
	}
	
	public long getErrorCount() {
		return errors.get();
	}
	
	public long getCompletedCount() {
		return completed.get();
	}
	
	private class ExceptionLoggingTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {
		private final RunnableScheduledFuture<V> task;
		
		public ExceptionLoggingTask(Callable<V> c, RunnableScheduledFuture<V> task) {
			super(c);
			this.task = task;
		}
		
		public ExceptionLoggingTask(Runnable r, RunnableScheduledFuture<V> task) {
			super(r, null);
			this.task = task;
		}
		
		/*
		 * Do not override this method to do exception logging on SE 6 or less!
		 * 
		 * FutureTask breaks its contract with respect to this method.  The
		 * Javadoc for this method says: "This method is invoked internally by
		 * the run method upon failure of the computation."  This is false.
		 * 
		 * The result: this method is never called by FutureTask when an
		 * Exception is caught during task execution, contrary to the Javadoc.
		 * 
		 * Sun "fixed" this for JDK 7b07 in bug #6464365, but apparently no
		 * bug was delivered for JDK 6.  So they're just leaving the Javadoc
		 * misleading and the implementation broken for JDK 6.
		 */
//		@Override
//		protected void setException(Throwable t) {
//			super.setException(t);
//			log.debug("Task terminated abnormally due to exception", t);
//			errors.incrementAndGet();
//		}
		
		/*
		 * I only override done to log exceptions.  Ideally I could override
		 * the setException() method for this, but as documented above, that
		 * method is broken in JDK 6.
		 * 
		 * This implementation was adapted from 
		 * http://stackoverflow.com/questions/3555302/how-to-catch-exceptions-in-futuretask
		 * 
		 * Although it's a bad idea in general to override a method without
		 * calling the super implementation, here it is safe due to the
		 * implementation details of FutureTask.  The Javadoc for FutureTask
		 * is correct when it says that the default implementation does
		 * nothing (according to my reading of the FutureTask source).
		 */
	    @Override
	    protected void done() {
	        try {
	            if (super.isDone() && !isCancelled()) get();
	        } catch (ExecutionException e) {
	            log.warn("Task terminated abnormally", e.getCause());
	            errors.incrementAndGet();
	        } catch (InterruptedException e) {
	            // Shouldn't happen, we're invoked when computation is finished
	            throw new AssertionError(e);
	        }
	    }


		// Wrapper methods around task
		
		@Override
		public long getDelay(TimeUnit unit) {
			return task.getDelay(unit);
		}

		@Override
		public int compareTo(Delayed o) {
			return task.compareTo(o);
		}

		@Override
		public boolean isPeriodic() {
			return task.isPeriodic();
		}
	}
		
//		@Override
//		public void run() {
//			task.run();
//		}
//
//		@Override
//		public boolean cancel(boolean mayInterruptIfRunning) {
//			return task.cancel(mayInterruptIfRunning);
//		}
//
//		@Override
//		public boolean isCancelled() {
//			return task.isCancelled();
//		}
//
//		@Override
//		public boolean isDone() {
//			return task.isDone();
//		}
//
//		@Override
//		public V get() throws InterruptedException, ExecutionException {
//			return task.get();
//		}
//
//		@Override
//		public V get(long timeout, TimeUnit unit) throws InterruptedException,
//				ExecutionException, TimeoutException {
//			return task.get(timeout, unit);
//		}
//
//		@Override
//		public long getDelay(TimeUnit unit) {
//			return task.getDelay(unit);
//		}
//
//		@Override
//		public int compareTo(Delayed o) {
//			return task.compareTo(o);
//		}
//
//		@Override
//		public boolean isPeriodic() {
//			return task.isPeriodic();
//		}

}