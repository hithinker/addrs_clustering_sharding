
package com.btc.paper.graph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.Math;

// GraphPartition implements the function of partitioning a graph to multiple
// clusters. Input of Partition is a hash map representing the connect table,
// and the number of clusters.
public class GraphPartition {
	// the float list consists of pairs of id and weight, id can be converted to int
	private static HashMap<Integer, ArrayList<Integer>> connect_table;
	private static HashMap<Integer, ArrayList<Float>> weight_table;
	
	// here a long represents two integer index of nodes appending by ascending order
	private static HashMap<Long, Float> edges;
	private static HashSet<Integer> nodes;
	private static HashMap<Integer, Float> nodeWeightSum;
	
	private static double weightSum;
		
	public static HashMap<Integer, Integer> Partition(int clusterNum,int round) {
		
		//ArrayList<HashSet<Integer>> result = new ArrayList<HashSet<Integer>>();
		HashMap<Integer, Integer> output = new HashMap<Integer, Integer>();
		
		// first extract all edges/nodes from the graph to form connect table and compute weightSum.
		long gStart = System.currentTimeMillis();
		generateStructures(round);
		long gEnd = System.currentTimeMillis();
		System.out.println("generate structures cost " + (gEnd - gStart) + "ms.");
		int nodeNum = nodes.size();
		System.out.println("new partition start with " + nodeNum + " nodes, " + edges.size() + " edges, and weightSum " + weightSum);
		
		ArrayList<HashMap<Integer, Float>> remainsWeightToCluster = new ArrayList<HashMap<Integer, Float>>();
		
		long start = System.currentTimeMillis();
		// begin of the cluster partition
		for (int i = 0; i < clusterNum; ++i) {
			long cStart = System.currentTimeMillis();
			HashSet<Integer> cluster = new HashSet<Integer>();
			HashMap<Integer, Float> weightToCluster = new HashMap<Integer, Float>();
			double innerWeight = 0;
			double partitionWeight = 0;
			
			while (innerWeight < weightSum / (clusterNum + 1) && cluster.size() < nodeNum / (clusterNum + 1) && edges.isEmpty() == false) {
				// choose the max weight edge
				long startNodes = 0;
				double startWeight = 0;
				for (Map.Entry<Long, Float> entry : edges.entrySet()) {
					if (entry.getValue() > startWeight) {
						startWeight = entry.getValue();
						startNodes = entry.getKey();
					}
				}
				innerWeight += startWeight;
				edges.remove(startNodes);
				
				// add the first two nodes to cluster
				int node1 = (int)(startNodes >> 32);
				int node2 = (int)startNodes;
				nodes.remove(node1);
				nodes.remove(node2);
				cluster.add(node1);
				cluster.add(node2);

				ArrayList<Integer> connections = connect_table.get(node1);
				ArrayList<Float> weights = weight_table.get(node1);
				for (int j = 0; j < connections.size(); j++) {
					int another = connections.get(j);
					float weight = weights.get(j);
					if (cluster.contains(another) == false) {
						partitionWeight += weight;
						if (nodes.contains(another)) {
							long edgeIndex = (((long)(Math.min(node1, another))) << 32)
									+ ((long)(Math.max(node1, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + weight);
							else
								weightToCluster.put(another, weight);
						}
					}
				}
				connections = connect_table.get(node2);
				weights = weight_table.get(node2);
				for (int j = 0; j < connections.size(); j++) {
					int another = connections.get(j);
					float weight = weights.get(j);
					if (cluster.contains(another) == false) {
						partitionWeight += weight;
						if (nodes.contains(another)) {
							long edgeIndex = (((long)(Math.min(node2, another))) << 32)
									+ ((long)(Math.max(node2, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + weight);
							else
								weightToCluster.put(another, weight);
						}
					}
				}
				
				// cluster grows, note that the partition can end before weight reach the set value
				// if there is no more node connected to the cluster.
				while (innerWeight < weightSum / (clusterNum + 1) && weightToCluster.isEmpty() == false && cluster.size() < nodeNum / (clusterNum + 1)) {
					int newNode = 0;
					float newNodeWeightToCluster = 0;
					for (Map.Entry<Integer, Float> entry : weightToCluster.entrySet()) {
						if (entry.getValue() > newNodeWeightToCluster) {
							newNodeWeightToCluster = entry.getValue();
							newNode = entry.getKey();
						}
					}
					weightToCluster.remove(newNode);
					nodes.remove(newNode);
					cluster.add(newNode);
					connections = connect_table.get(newNode);
					weights = weight_table.get(newNode);
					for (int j = 0; j < connections.size(); j++) {
						int another = connections.get(j);
						float weight = weights.get(j);
						if (cluster.contains(another) == false) {
							partitionWeight += weight;
							if (nodes.contains(another)) {
								long edgeIndex = (((long)(Math.min(newNode, another))) << 32)
										+ ((long)(Math.max(newNode, another)));
								edges.remove(edgeIndex);
								if (weightToCluster.containsKey(another))
									weightToCluster.put(another, weightToCluster.get(another) + weight);
								else
									weightToCluster.put(another, weight);
							}
						}
					}
					innerWeight += newNodeWeightToCluster;
					partitionWeight -= newNodeWeightToCluster;
				}
			}
			if (edges.isEmpty() == true) {
				for (int node : nodes) {
					if (innerWeight >= weightSum / (clusterNum + 1) || cluster.size() >= nodeNum / (clusterNum + 1))
						break;
					cluster.add(node);
					ArrayList<Integer> connections = connect_table.get(node);
					ArrayList<Float> weights = weight_table.get(node);
					for (int j = 0; j < connections.size(); j++) {
						int another = connections.get(j);
						float weight = weights.get(j);
						if (cluster.contains(another) == false) {
							partitionWeight += weight;
						}
					}
				}
				nodes.removeAll(cluster);
			}
			remainsWeightToCluster.add(weightToCluster);
			//result.add(cluster);
			for (int node : cluster) {
				output.put(node, i);
			}
			long cEnd = System.currentTimeMillis();
			System.out.println(i + "th partition costs:" + (cEnd - cStart) + "ms.");
			System.out.println("cluster " + i + " with " + cluster.size() + " nodes, partitionW:" + partitionWeight + ", innerW:" + innerWeight);
		}
		
		// the remaining nodes is allocated to a cluster with biggest weight
		for (int node : nodes) {
			int choice = node % clusterNum;
			float weight = 0;
			for (int i = 0; i < clusterNum; ++i) {
				if (remainsWeightToCluster.get(i).containsKey(node) && remainsWeightToCluster.get(i).get(node) > weight) {
					weight = remainsWeightToCluster.get(i).get(node);
					choice = i;
				}
			}
			//HashSet<Integer> newCluster = result.get(choice);
			//newCluster.add(node);
			//result.set(choice, newCluster);
			output.put(node, choice);
		}
		long end = System.currentTimeMillis();
		System.out.println("graph Partition costs:" + (end - start));
		return output;
	}
	
	public static HashMap<Integer, Integer> Partition2(int clusterNum,int round) {
		
		HashMap<Integer, Integer> output = new HashMap<Integer, Integer>();
		
		// first extract all edges/nodes from the graph to form connect table and compute weightSum.
		generateStructures(round);
		int nodeNum = nodes.size();
		System.out.println("new partition start with " + nodeNum + " nodes, " + edges.size() + " edges, and weightSum " + weightSum);
		
		ArrayList<HashMap<Integer, Float>> remainsWeightToCluster = new ArrayList<HashMap<Integer, Float>>();
		
		long start = System.currentTimeMillis();
		// begin of the cluster partition
		for (int i = 0; i < clusterNum; ++i) {
			long cStart = System.currentTimeMillis();
			HashSet<Integer> cluster = new HashSet<Integer>();
			HashMap<Integer, Float> weightToCluster = new HashMap<Integer, Float>();
			double innerWeight = 0;
			double partitionWeight = 0;
			
			while (innerWeight < weightSum / (clusterNum + 1) && cluster.size() < nodeNum / (clusterNum + 1) && edges.isEmpty() == false) {
				// choose the max weight edge
				long startNodes = 0;
				double startWeight = 0;
				for (Map.Entry<Long, Float> entry : edges.entrySet()) {
					if (entry.getValue() > startWeight) {
						startWeight = entry.getValue();
						startNodes = entry.getKey();
					}
				}
				innerWeight += startWeight;
				edges.remove(startNodes);
				
				// add the first two nodes to cluster
				int node1 = (int)(startNodes >> 32);
				int node2 = (int)startNodes;
				nodes.remove(node1);
				nodes.remove(node2);
				cluster.add(node1);
				cluster.add(node2);

				ArrayList<Integer> connections = connect_table.get(node1);
				ArrayList<Float> weights = weight_table.get(node1);
				for (int j = 0; j < connections.size(); j++) {
					int another = connections.get(j);
					float weight = weights.get(j);
					if (cluster.contains(another) == false) {
						partitionWeight += weight;
						if (nodes.contains(another)) {
							long edgeIndex = (((long)(Math.min(node1, another))) << 32)
									+ ((long)(Math.max(node1, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + weight);
							else
								weightToCluster.put(another, weight);
						}
					}
				}
				connections = connect_table.get(node2);
				weights = weight_table.get(node2);
				for (int j = 0; j < connections.size(); j++) {
					int another = connections.get(j);
					float weight = weights.get(j);
					if (cluster.contains(another) == false) {
						partitionWeight += weight;
						if (nodes.contains(another)) {
							long edgeIndex = (((long)(Math.min(node2, another))) << 32)
									+ ((long)(Math.max(node2, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + weight);
							else
								weightToCluster.put(another, weight);
						}
					}
				}
				
				// cluster grows, note that the partition can end before weight reach the set value
				// if there is no more node connected to the cluster.
				while (innerWeight < weightSum / (clusterNum + 1) && weightToCluster.isEmpty() == false && cluster.size() < nodeNum / (clusterNum + 1)) {
					int newNode = 0;
					float newNodeWeightToCluster = 0;
					float weightProportion = 0;
					for (Map.Entry<Integer, Float> entry : weightToCluster.entrySet()) {
						if (entry.getValue() / (nodeWeightSum.get(entry.getKey())) > weightProportion) {
							weightProportion = entry.getValue() / (nodeWeightSum.get(entry.getKey()));
							newNodeWeightToCluster = entry.getValue();
							newNode = entry.getKey();
						}
					}
					weightToCluster.remove(newNode);
					nodes.remove(newNode);
					cluster.add(newNode);
					connections = connect_table.get(newNode);
					weights = weight_table.get(newNode);
					for (int j = 0; j < connections.size(); j++) {
						int another = connections.get(j);
						float weight = weights.get(j);
						if (cluster.contains(another) == false) {
							partitionWeight += weight;
							if (nodes.contains(another)) {
								long edgeIndex = (((long)(Math.min(newNode, another))) << 32)
										+ ((long)(Math.max(newNode, another)));
								edges.remove(edgeIndex);
								if (weightToCluster.containsKey(another))
									weightToCluster.put(another, weightToCluster.get(another) + weight);
								else
									weightToCluster.put(another, weight);
							}
						}
					}
					innerWeight += newNodeWeightToCluster;
					partitionWeight -= newNodeWeightToCluster;
				}
			}
			if (edges.isEmpty() == true) {
				for (int node : nodes) {
					if (cluster.size() >= nodeNum / (clusterNum + 1))
						break;
					cluster.add(node);
					ArrayList<Integer> connections = connect_table.get(node);
					ArrayList<Float> weights = weight_table.get(node);
					for (int j = 0; j < connections.size(); j++) {
						int another = connections.get(j);
						float weight = weights.get(j);
						if (cluster.contains(another) == false) {
							partitionWeight += weight;
						}
					}
				}
				nodes.removeAll(cluster);
			}
			remainsWeightToCluster.add(weightToCluster);
			//result.add(cluster);
			for (int node : cluster) {
				output.put(node, i);
			}
			long cEnd = System.currentTimeMillis();
			System.out.println(i + "th partition costs:" + (cEnd - cStart) + "ms.");
			System.out.println("cluster " + i + " with " + cluster.size() + " nodes, partitionW:" + partitionWeight + ", innerW:" + innerWeight);
		}
		
		// the remaining nodes is allocated to a cluster with biggest weight
		for (int node : nodes) {
			int choice = node % clusterNum;
			float weight = 0;
			for (int i = 0; i < clusterNum; ++i) {
				if (remainsWeightToCluster.get(i).containsKey(node) && remainsWeightToCluster.get(i).get(node) > weight) {
					weight = remainsWeightToCluster.get(i).get(node);
					choice = i;
				}
			}
			//HashSet<Integer> newCluster = result.get(choice);
			//newCluster.add(node);
			//result.set(choice, newCluster);
			output.put(node, choice);
		}
		long end = System.currentTimeMillis();
		System.out.println("graph Partition costs:" + (end - start));
		return output;
	}
	
public static HashMap<Integer, Integer> Partition3(int clusterNum,int round) {
		
		HashMap<Integer, Integer> output = new HashMap<Integer, Integer>();
		
		// first extract all edges/nodes from the graph to form connect table and compute weightSum.
		generateStructures(round);
		int nodeNum = nodes.size();
		System.out.println("new partition start with " + nodeNum + " nodes, " + edges.size() + " edges, and weightSum " + weightSum);
		
		ArrayList<HashMap<Integer, Float>> remainsWeightToCluster = new ArrayList<HashMap<Integer, Float>>();
		
		long start = System.currentTimeMillis();
		// begin of the cluster partition
		for (int i = 0; i < clusterNum; ++i) {
			long cStart = System.currentTimeMillis();
			HashSet<Integer> cluster = new HashSet<Integer>();
			HashMap<Integer, Float> weightToCluster = new HashMap<Integer, Float>();
			double innerWeight = 0;
			double partitionWeight = 0;
			
			while (cluster.size() < nodeNum / (clusterNum + 1) && edges.isEmpty() == false) {
				// choose the max weight edge
				int startNode = 0;
				double startWeight = 0;
				for (Map.Entry<Integer, Float> entry : nodeWeightSum.entrySet()) {
					if (entry.getValue() > startWeight) {
						startWeight = entry.getValue();
						startNode = entry.getKey();
					}
				}
				
				// add the first node to cluster
				nodes.remove(startNode);
				nodeWeightSum.remove(startNode);
				cluster.add(startNode);

				ArrayList<Integer> connections = connect_table.get(startNode);
				ArrayList<Float> weights = weight_table.get(startNode);
				for (int j = 0; j < connections.size(); j++) {
					int another = connections.get(j);
					float weight = weights.get(j);
					if (cluster.contains(another) == false) {
						partitionWeight += weight;
						if (nodes.contains(another)) {
							long edgeIndex = (((long)(Math.min(startNode, another))) << 32)
									+ ((long)(Math.max(startNode, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + weight);
							else
								weightToCluster.put(another, weight);
						}
					}
				}
				
				// cluster grows, note that the partition can end before weight reach the set value
				// if there is no more node connected to the cluster.
				while (weightToCluster.isEmpty() == false && cluster.size() < nodeNum / (clusterNum + 1)) {
					int newNode = 0;
					float newNodeWeightToCluster = 0;
					float weightProportion = 0;
					for (Map.Entry<Integer, Float> entry : weightToCluster.entrySet()) {
						if (entry.getValue() / (nodeWeightSum.get(entry.getKey())) > weightProportion) {
							weightProportion = entry.getValue() / (nodeWeightSum.get(entry.getKey()));
							newNodeWeightToCluster = entry.getValue();
							newNode = entry.getKey();
						}
					}
					weightToCluster.remove(newNode);
					nodes.remove(newNode);
					nodeWeightSum.remove(newNode);
					cluster.add(newNode);
					connections = connect_table.get(newNode);
					weights = weight_table.get(newNode);
					for (int j = 0; j < connections.size(); j++) {
						int another = connections.get(j);
						float weight = weights.get(j);
						if (cluster.contains(another) == false) {
							partitionWeight += weight;
							if (nodes.contains(another)) {
								long edgeIndex = (((long)(Math.min(newNode, another))) << 32)
										+ ((long)(Math.max(newNode, another)));
								edges.remove(edgeIndex);
								if (weightToCluster.containsKey(another))
									weightToCluster.put(another, weightToCluster.get(another) + weight);
								else
									weightToCluster.put(another, weight);
							}
						}
					}
					innerWeight += newNodeWeightToCluster;
					partitionWeight -= newNodeWeightToCluster;
				}
			}
			if (edges.isEmpty() == true) {
				for (int node : nodes) {
					if (cluster.size() >= nodeNum / (clusterNum + 1))
						break;
					cluster.add(node);
					ArrayList<Integer> connections = connect_table.get(node);
					ArrayList<Float> weights = weight_table.get(node);
					for (int j = 0; j < connections.size(); j++) {
						int another = connections.get(j);
						float weight = weights.get(j);
						if (cluster.contains(another) == false) {
							partitionWeight += weight;
						}
					}
				}
				nodes.removeAll(cluster);
			}
			remainsWeightToCluster.add(weightToCluster);
			//result.add(cluster);
			for (int node : cluster) {
				output.put(node, i);
			}
			long cEnd = System.currentTimeMillis();
			System.out.println(i + "th partition costs:" + (cEnd - cStart) + "ms.");
			System.out.println("cluster " + i + " with " + cluster.size() + " nodes, partitionW:" + partitionWeight + ", innerW:" + innerWeight);
		}
		
		// the remaining nodes is allocated to a cluster with biggest weight
		for (int node : nodes) {
			int choice = node % clusterNum;
			float weight = 0;
			for (int i = 0; i < clusterNum; ++i) {
				if (remainsWeightToCluster.get(i).containsKey(node) && remainsWeightToCluster.get(i).get(node) > weight) {
					weight = remainsWeightToCluster.get(i).get(node);
					choice = i;
				}
			}
			//HashSet<Integer> newCluster = result.get(choice);
			//newCluster.add(node);
			//result.set(choice, newCluster);
			output.put(node, choice);
		}
		long end = System.currentTimeMillis();
		System.out.println("graph Partition costs:" + (end - start));
		return output;
	}

public static HashMap<Integer, Integer> Partition4(int clusterNum,int round) {
	
	HashMap<Integer, Integer> output = new HashMap<Integer, Integer>();
	
	// first extract all edges/nodes from the graph to form connect table and compute weightSum.
	long gStart = System.currentTimeMillis();
	generateStructures(round);
	long gEnd = System.currentTimeMillis();
	System.out.println("generate structures cost " + (gEnd - gStart) + "ms.");
	int nodeNum = nodes.size();
	System.out.println("new partition start with " + nodeNum + " nodes, " + edges.size() + " edges, and weightSum " + weightSum);
	
	ArrayList<HashMap<Integer, Float>> remainsWeightToCluster = new ArrayList<HashMap<Integer, Float>>();
	
	long start = System.currentTimeMillis();
	// begin of the cluster partition
	for (int i = 0; i < clusterNum; ++i) {
		long cStart = System.currentTimeMillis();
		HashSet<Integer> cluster = new HashSet<Integer>();
		HashMap<Integer, Float> weightToCluster = new HashMap<Integer, Float>();
		double innerWeight = 0;
		double partitionWeight = 0;
		
		while (((innerWeight < partitionWeight && partitionWeight > 10000) || 
				(cluster.size() < nodes.size() / (clusterNum - i + 1))) && edges.isEmpty() == false) {
			// choose the max weight edge
			int startNode = 0;
			double startWeight = 0;
			for (Map.Entry<Integer, Float> entry : nodeWeightSum.entrySet()) {
				if (entry.getValue() > startWeight) {
					startWeight = entry.getValue();
					startNode = entry.getKey();
				}
			}
			
			// add the first node to cluster
			nodes.remove(startNode);
			nodeWeightSum.remove(startNode);
			cluster.add(startNode);

			ArrayList<Integer> connections = connect_table.get(startNode);
			ArrayList<Float> weights = weight_table.get(startNode);
			for (int j = 0; j < connections.size(); j++) {
				int another = connections.get(j);
				float weight = weights.get(j);
				if (cluster.contains(another) == false) {
					partitionWeight += weight;
					if (nodes.contains(another)) {
						long edgeIndex = (((long)(Math.min(startNode, another))) << 32)
								+ ((long)(Math.max(startNode, another)));
						edges.remove(edgeIndex);
						if (weightToCluster.containsKey(another))
							weightToCluster.put(another, weightToCluster.get(another) + weight);
						else
							weightToCluster.put(another, weight);
					}
				}
			}
			
			// cluster grows, note that the partition can end before weight reach the set value
			// if there is no more node connected to the cluster.
			while (weightToCluster.isEmpty() == false && ((innerWeight < partitionWeight && partitionWeight > 10000) ||
					(cluster.size() < nodes.size() / (clusterNum - i + 1)))) {
				int newNode = 0;
				float newNodeWeightToCluster = 0;
				//float weightProportion = 0;
				for (Map.Entry<Integer, Float> entry : weightToCluster.entrySet()) {
					if (entry.getValue() > newNodeWeightToCluster) {// / (nodeWeightSum.get(entry.getKey())) > weightProportion) {
						//weightProportion = entry.getValue() / (nodeWeightSum.get(entry.getKey()));
						newNodeWeightToCluster = entry.getValue();
						newNode = entry.getKey();
					}
				}
				weightToCluster.remove(newNode);
				nodes.remove(newNode);
				nodeWeightSum.remove(newNode);
				cluster.add(newNode);
				connections = connect_table.get(newNode);
				weights = weight_table.get(newNode);
				for (int j = 0; j < connections.size(); j++) {
					int another = connections.get(j);
					float weight = weights.get(j);
					if (cluster.contains(another) == false) {
						partitionWeight += weight;
						if (nodes.contains(another)) {
							long edgeIndex = (((long)(Math.min(newNode, another))) << 32)
									+ ((long)(Math.max(newNode, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + weight);
							else
								weightToCluster.put(another, weight);
						}
					}
				}
				innerWeight += newNodeWeightToCluster;
				partitionWeight -= newNodeWeightToCluster;
			}
		}
		if (edges.isEmpty() == true) {
			for (int node : nodes) {
				if (cluster.size() >= nodes.size() / (clusterNum - i + 1))
					break;
				cluster.add(node);
				ArrayList<Integer> connections = connect_table.get(node);
				ArrayList<Float> weights = weight_table.get(node);
				for (int j = 0; j < connections.size(); j++) {
					int another = connections.get(j);
					float weight = weights.get(j);
					if (cluster.contains(another) == false) {
						partitionWeight += weight;
					}
				}
			}
			nodes.removeAll(cluster);
		}
		remainsWeightToCluster.add(weightToCluster);
		//result.add(cluster);
		for (int node : cluster) {
			output.put(node, i);
		}
		long cEnd = System.currentTimeMillis();
		System.out.println(i + "th partition costs:" + (cEnd - cStart) + "ms.");
		System.out.println("cluster " + i + " with " + cluster.size() + " nodes, partitionW:" + partitionWeight + ", innerW:" + innerWeight);
	}
	
	// the remaining nodes is allocated to a cluster with biggest weight
	for (int node : nodes) {
		int choice = node % clusterNum;
		float weight = 0;
		for (int i = 0; i < clusterNum; ++i) {
			if (remainsWeightToCluster.get(i).containsKey(node) && remainsWeightToCluster.get(i).get(node) > weight) {
				weight = remainsWeightToCluster.get(i).get(node);
				choice = i;
			}
		}
		//HashSet<Integer> newCluster = result.get(choice);
		//newCluster.add(node);
		//result.set(choice, newCluster);
		output.put(node, choice);
	}
	long end = System.currentTimeMillis();
	System.out.println("graph Partition costs:" + (end - start));
	return output;
}

public static HashMap<Integer, Integer> Partition5(int clusterNum,int round) {
	
	HashMap<Integer, Integer> output = new HashMap<Integer, Integer>();
	
	// first extract all edges/nodes from the graph to form connect table and compute weightSum.
	long gStart = System.currentTimeMillis();
	generateStructures(round);
	long gEnd = System.currentTimeMillis();
	System.out.println("generate structures cost " + (gEnd - gStart) + "ms.");
	int nodeNum = nodes.size();
	System.out.println("new partition start with " + nodeNum + " nodes, " + edges.size() + " edges, and weightSum " + weightSum);
	
	ArrayList<HashMap<Integer, Float>> remainsWeightToCluster = new ArrayList<HashMap<Integer, Float>>();
	
	long start = System.currentTimeMillis();
	// begin of the cluster partition
	for (int i = 0; i < clusterNum; ++i) {
		long cStart = System.currentTimeMillis();
		HashSet<Integer> cluster = new HashSet<Integer>();
		HashMap<Integer, Float> weightToCluster = new HashMap<Integer, Float>();
		double innerWeight = 0;
		double partitionWeight = 0;
		
		for (int node : nodes) {
			if (cluster.size() >= nodes.size() / (clusterNum - i + 1))
				break;
			cluster.add(node);
			ArrayList<Integer> connections = connect_table.get(node);
			ArrayList<Float> weights = weight_table.get(node);
			for (int j = 0; j < connections.size(); j++) {
				int another = connections.get(j);
				float weight = weights.get(j);
				if (cluster.contains(another) == false) {
					partitionWeight += weight;
				}
				else {
					partitionWeight -= weight;
					innerWeight += weight;
				}
			}
		}
		nodes.removeAll(cluster);
		remainsWeightToCluster.add(weightToCluster);
		//result.add(cluster);
		for (int node : cluster) {
			output.put(node, i);
		}
		long cEnd = System.currentTimeMillis();
		System.out.println(i + "th partition costs:" + (cEnd - cStart) + "ms.");
		System.out.println("cluster " + i + " with " + cluster.size() + " nodes, partitionW:" + partitionWeight + ", innerW:" + innerWeight);
	}
	
	// the remaining nodes is allocated to a cluster with biggest weight
	for (int node : nodes) {
		int choice = node % clusterNum;
		float weight = 0;
		for (int i = 0; i < clusterNum; ++i) {
			if (remainsWeightToCluster.get(i).containsKey(node) && remainsWeightToCluster.get(i).get(node) > weight) {
				weight = remainsWeightToCluster.get(i).get(node);
				choice = i;
			}
		}
		//HashSet<Integer> newCluster = result.get(choice);
		//newCluster.add(node);
		//result.set(choice, newCluster);
		output.put(node, choice);
	}
	long end = System.currentTimeMillis();
	System.out.println("graph Partition costs:" + (end - start));
	return output;
}
	
	// generate edges, nodes and compute the weight sum of all edges in the same time
	private static void generateStructures(int round) {
		connect_table = new HashMap<Integer, ArrayList<Integer>>();
		weight_table = new HashMap<Integer, ArrayList<Float>>();
		long t1 = System.currentTimeMillis();
		Graph graph = new Graph();
		graph.createGraph(round);
		HashMap<Integer,ArrayList<Float>> pair_table = graph.getEdgesInfos();
		long t2 = System.currentTimeMillis();
		System.out.println("generate graph stage:" + (t2 - t1) + "ms.");
		edges = new HashMap<Long, Float>();
		nodes = new HashSet<Integer>();
		nodeWeightSum = new HashMap<Integer, Float>();
		weightSum = 0;
		for (Map.Entry<Integer, ArrayList<Float>> entry : pair_table.entrySet()) {
			int thisEnd = entry.getKey();
			float thisWeightSum = 0;
			ArrayList<Float> pairs = entry.getValue();
			nodes.add(thisEnd);
			ArrayList<Integer> connections = new ArrayList<Integer>();
			ArrayList<Float> weights = new ArrayList<Float>();
			for (int i = 0; i < pairs.size(); i += 2) {
				int anotherEnd = pairs.get(i).intValue();
				float weight = pairs.get(i + 1);
				thisWeightSum += weight;
				connections.add(anotherEnd);
				weights.add(weight);
				long edgeIndex = (((long)(Math.min(thisEnd, anotherEnd))) << 32)
						+ ((long)(Math.max(thisEnd, anotherEnd)));
				if (edges.containsKey(edgeIndex) == false) {
					edges.put(edgeIndex, weight);
					weightSum += weight;
				}
			}
			nodeWeightSum.put(thisEnd, thisWeightSum);
			connect_table.put(thisEnd, connections);
			weight_table.put(thisEnd, weights);
		}
	}
	
	public static void degreeStatistics(int round) {
		System.out.println("node degree statistics for round " + round + ":");
		long t1 = System.currentTimeMillis();
		Graph graph = new Graph();
		graph.createGraph(round);
		HashMap<Integer,ArrayList<Float>> pair_table = graph.getEdgesInfos();
		long t2 = System.currentTimeMillis();
		System.out.println("generate graph stage:" + (t2 - t1) + "ms.");
		nodeWeightSum = new HashMap<Integer, Float>();
		weightSum = 0;
		for (Map.Entry<Integer, ArrayList<Float>> entry : pair_table.entrySet()) {
			int thisEnd = entry.getKey();
			float thisWeightSum = 0;
			ArrayList<Float> pairs = entry.getValue();
			for (int i = 0; i < pairs.size(); i += 2) {
				float weight = pairs.get(i + 1);
				thisWeightSum += weight;
			}
			nodeWeightSum.put(thisEnd, thisWeightSum);
		}
		ArrayList<Map.Entry<Integer, Float>> list = new ArrayList<Map.Entry<Integer, Float>>(nodeWeightSum.entrySet());
		list.sort(new Comparator<Map.Entry<Integer, Float>>() {
	          public int compare(Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2) {
	              return o2.getValue().compareTo(o1.getValue());
	          }
	      });
		for (Map.Entry<Integer, Float> pair : list) {
			System.out.println("node " + pair.getKey() + " is of degree " + pair.getValue());
		}
	}
	
	public static void freshClusters(HashMap<Integer,Integer> clusters,int round,int clusterNum) {
		if(round == 0) {
			saveIdCidMap(clusters);
			return; 
		}	
		String oldIdCidFilePath = "/home/infosec/sharding_expt/idCid" + (round - 1) + ".txt";
		String newidCidFilePath = "/home/infosec/sharding_expt/idCid" + round + ".txt";
		File oldIdCidFile = new File(oldIdCidFilePath);
		File newIdCidFile = new File(newidCidFilePath);
		if(!newIdCidFile.exists())
			try {
				newIdCidFile.createNewFile();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(oldIdCidFile)));
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newIdCidFile)));
			ArrayList<HashMap<Integer,Integer>> old2NewStats = new ArrayList<HashMap<Integer,Integer>>();
			ArrayList<ArrayList<Integer>> remainingAddrIds = new ArrayList<ArrayList<Integer>>();
			for(int i = 0;i<clusterNum;i++) {
				old2NewStats.add(new HashMap<Integer,Integer>());
				remainingAddrIds.add(new ArrayList<Integer>());
			}
			String idCidLine = null;
			while((idCidLine = br.readLine())!= null) {
				if(idCidLine.trim().length() < 1)
					break;
				int aId = Integer.parseInt(idCidLine.trim().split(" ")[0]);
				int cId = Integer.parseInt(idCidLine.trim().split(" ")[1]);
				if(clusters.containsKey(aId)) {
					HashMap<Integer,Integer> singleOldClusterStat = old2NewStats.get(cId);
					int newCid = clusters.get(aId);
					if(singleOldClusterStat.containsKey(newCid)) 
						singleOldClusterStat.put(newCid, singleOldClusterStat.get(newCid) + 1);		
					else
						singleOldClusterStat.put(newCid,1);
					String idCidPair = aId + " " + clusters.get(aId) + "\n";
					bw.write(idCidPair);
				}
				else {
					if(cId < 0 || cId > 1023)
						System.out.println(cId);
					remainingAddrIds.get(cId).add(aId);
				}
			}
			oldIdCidFile.delete();			
			for(int i = 0;i<old2NewStats.size();i++) {
				int maxValue = -1;
				int maxCid = -1;
				HashMap<Integer,Integer> oneOldClusterStat = old2NewStats.get(i);
				for(int clusterId:oneOldClusterStat.keySet()) {
					if(oneOldClusterStat.get(clusterId) > maxValue) {
						maxCid = clusterId;
						maxValue = oneOldClusterStat.get(clusterId);
					}
				}
				ArrayList<Integer> ids = remainingAddrIds.get(i);				
				for(int id:ids) {
					String idCidPair = id + " " + maxCid + "\n";
					bw.write(idCidPair);
				}
			}
			for(int addrId:clusters.keySet()) {
				String idCidPair = addrId + " " + clusters.get(addrId) + "\n";
				bw.write(idCidPair);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		}finally {
			try {
				if(br != null)
					br.close();
				if(bw != null)
					bw.close();
				}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void saveIdCidMap(HashMap<Integer, Integer> idCidPairs) {
		HashMap<Integer, Integer> idCidStat = new HashMap<Integer, Integer>();
		File idCidFilePath = new File("/home/infosec/sharding_expt/idCid0.txt");
		System.out.println("write to file /home/infosec/sharding_expt/idCid0.txt");
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(idCidFilePath)));
			if (!idCidFilePath.exists())
				idCidFilePath.createNewFile();			
            for(int id:idCidPairs.keySet()) {
            	int cId = idCidPairs.get(id);
            	String idCidStr = id + " " + cId + "\n";
            	if(!idCidStat.containsKey(cId))
            		idCidStat.put(cId, 1);
            	else
            		idCidStat.put(cId, idCidStat.get(cId) + 1);
            	bw.write(idCidStr);
		    }
            for(int cid:idCidStat.keySet()) {
            	System.out.println(cid + ":" + idCidStat.get(cid));
            }            
		}catch(FileNotFoundException e) {
			e.getStackTrace();
		} 
		catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}finally {			
			if(bw != null)
				try {
					bw.flush();
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
}
