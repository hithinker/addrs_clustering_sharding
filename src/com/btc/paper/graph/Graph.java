package com.btc.paper.graph;

import java.util.ArrayList;
import java.util.HashMap;

import com.btc.paper.data.BlockProcessor;

//方法:1.构图 
public class Graph {
	private HashMap<Integer,ArrayList<Float>> graph;
	public void createGraph(int round) {
		long start = System.currentTimeMillis();
		graph = new HashMap<Integer,ArrayList<Float>>();
		BlockProcessor blkPcs = new BlockProcessor();
		String dataDir = "data" + 200*round + "_" + (200*(round+1) - 1);
		HashMap<Integer,HashMap<Integer,Float>> graphEdges = blkPcs.readBlock("/home/infosec/sharding_expt/data/" + dataDir,round);
		int edgeCount = 0;
		for(int node:graph.keySet()) {
			edgeCount += graph.get(node).size();
		}
		System.out.println("参与图划分边数:"  + edgeCount);
	    for(int node:graphEdges.keySet()) {
	    	HashMap<Integer, Float> adjList = graphEdges.get(node);
	    	for(int endNode:adjList.keySet()) { 
	    		float weight = adjList.get(endNode);
	    		if(graph.containsKey(node)) {
	    			graph.get(node).add((float) endNode);
	    			graph.get(node).add(weight);
	    		}
	    		else {
	    			ArrayList<Float> adjL = new ArrayList<Float>();
	    			adjL.add((float) endNode);
	    			adjL.add(weight);
	    			graph.put(node, adjL);
	    		}
	    		if(graph.containsKey(endNode)) {
	    			graph.get(endNode).add((float) node);
	    			graph.get(endNode).add(weight);
	    		}
	    		else {
	    			ArrayList<Float> adjL = new ArrayList<Float>();
	    			adjL.add((float) node);
	    			adjL.add(weight);
	    			graph.put(endNode, adjL);
	    	}
	    	}
	    }
	    long end = System.currentTimeMillis();
	    System.out.println("create graph costs:" + (end - start));
	}
	public HashMap<Integer,ArrayList<Float>>  getEdgesInfos(){
		return graph;
	}
}
