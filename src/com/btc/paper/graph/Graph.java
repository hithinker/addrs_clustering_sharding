package com.btc.paper.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.io.*;

//·½·¨:1.¹¹Í¼ 
public class Graph {
	private HashMap<Integer,ArrayList<Float>> graph;
	public void createGraph() {
		//DBOperation dbOp = new DBOperation();
		//graph = dbOp.getEpochEdges(2);
		graph = new HashMap<Integer,ArrayList<Float>>();
		try (FileReader reader = new FileReader("./test/testInput.txt");
	         BufferedReader br = new BufferedReader(reader)
	    ) {
			String line;
			while ((line = br.readLine()) != null) {
				String entry[] = line.split(" ");
				ArrayList<Float> list = new ArrayList<Float>();
				for (int i = 1; i < entry.length; ++i) {
					list.add(Float.parseFloat(entry[i]));
				}
				graph.put(Integer.parseInt(entry[0]), list);
			}
	    }
		catch (IOException e) {
            e.printStackTrace();
	    }
	}
	public HashMap<Integer,ArrayList<Float>>  getEdgesInfos(){
		return graph;
	}
}
