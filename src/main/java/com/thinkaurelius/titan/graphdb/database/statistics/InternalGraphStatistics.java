package com.thinkaurelius.titan.graphdb.database.statistics;

public interface InternalGraphStatistics extends GraphStatistics {

	/**
	 * Updates this graph statistics with incremental count updates due to a transaction commit.
	 * 
	 * @param stats Incremental statistical update to incorporate into this statistics.
	 */
	public void update(TransactionStatistics stats);
	
}
