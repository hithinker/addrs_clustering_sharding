package addrs_clustering_sharding;

import java.util.ArrayList;
import java.util.HashMap;

import DBUtil.DBOperation;

//����:1.��ͼ 
public class Graph {
	private HashMap<Integer,ArrayList<Float>> graph;
	public void createGraph() {
		DBOperation dbOp = new DBOperation();		
		graph = dbOp.getEpochEdges(3);
	}
	public HashMap<Integer,ArrayList<Float>>  getEdgesInfos(){
		return graph;
	}
}
