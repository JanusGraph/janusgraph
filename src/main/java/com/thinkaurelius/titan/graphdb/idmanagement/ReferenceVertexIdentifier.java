package com.thinkaurelius.titan.graphdb.idmanagement;

public interface ReferenceVertexIdentifier {

	public static ReferenceVertexIdentifier NO_REFERENCE_NODES = new ReferenceVertexIdentifier(){

		@Override
		public boolean isReferenceVertexID(long vid) {
			return false;
		}
		
	};
	
	public static ReferenceVertexIdentifier ALL_REFERENCE_NODES = new ReferenceVertexIdentifier(){

		@Override
		public boolean isReferenceVertexID(long vid) {
			return true;
		}
		
	};
	
	public boolean isReferenceVertexID(long vid);
}
