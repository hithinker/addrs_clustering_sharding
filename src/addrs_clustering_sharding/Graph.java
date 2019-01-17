package addrs_clustering_sharding;

import java.util.HashMap;

import DBUtil.DBOperation;

//·½·¨:1.¹¹Í¼ 
public class Graph {
	private HashMap<Integer,NodeEdgeInfo> graph;
	public void createGraph() {
		DBOperation dbOp = new DBOperation();
		graph = dbOp.getEpochEdges();
	}
	public HashMap<Integer,NodeEdgeInfo>  getEdgesInfos(){
		return null;
	}
}
