package com.btc.paper.graph;

import java.util.ArrayList;
import java.util.HashMap;

import com.btc.paper.data.BlockProcessor;

//·½·¨:1.¹¹Í¼ 
public class Graph {
	private HashMap<Integer,ArrayList<Float>> graph;

	public void createGraph(int round) {
		BlockProcessor blkPcs = new BlockProcessor();
		String dataDir = "data" + 200 * round + "_" + (200 * (round + 1) - 1);
		graph = blkPcs
				.readBlock("/home/infosec/sharding_expt/data/" + dataDir, round);
	}
	public HashMap<Integer,ArrayList<Float>>  getEdgesInfos(){
		return graph;
	}
}
