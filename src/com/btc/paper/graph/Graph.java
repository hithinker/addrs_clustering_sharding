package com.btc.paper.graph;

import java.util.ArrayList;
import java.util.HashMap;

import com.btc.paper.data.BlockProcessor;

//·½·¨:1.¹¹Í¼ 
public class Graph {
	private HashMap<Integer,ArrayList<Float>> graph;
	public void createGraph(int round) {
		graph = new HashMap<Integer,ArrayList<Float>>();
		BlockProcessor blkPcs = new BlockProcessor();
		HashMap<int[],Float> graphEdges = blkPcs.readBlock("/home/infosec/sharding_expt/data0_999",round);
	    for(int[] nodes:graphEdges.keySet()) {
	    	int node1 = nodes[0];
	    	int node2 = nodes[1];
	    	float weight = graphEdges.get(nodes);
	    	if(graph.containsKey(node1)) {
	    		graph.get(node1).add((float) node2);
	    		graph.get(node1).add(weight);
	    	}
	    	else {
	    		ArrayList<Float> adjList = new ArrayList<Float>();
	    		adjList.add((float) node2);
	    		adjList.add(weight);
	    		graph.put(node1, adjList);
	    	}
	    	if(graph.containsKey(node2)) {
	    		graph.get(node2).add((float) node1);
	    		graph.get(node2).add(weight);
	    	}
	    	else {
	    		ArrayList<Float> adjList = new ArrayList<Float>();
	    		adjList.add((float) node1);
	    		adjList.add(weight);
	    		graph.put(node2, adjList);
	    	}
	    }
	}
	public HashMap<Integer,ArrayList<Float>>  getEdgesInfos(){
		return graph;
	}
}
