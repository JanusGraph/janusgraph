package com.thinkaurelius.titan.graphdb.idmanagement;

public interface ReferenceNodeIdentifier{

	public static ReferenceNodeIdentifier noReferenceNodes = new ReferenceNodeIdentifier(){

		@Override
		public boolean isReferenceNodeID(long vid) {
			return false;
		}
		
	};
	
	public static ReferenceNodeIdentifier allReferenceNodes = new ReferenceNodeIdentifier(){

		@Override
		public boolean isReferenceNodeID(long vid) {
			return true;
		}
		
	};
	
	public boolean isReferenceNodeID(long vid);
}
