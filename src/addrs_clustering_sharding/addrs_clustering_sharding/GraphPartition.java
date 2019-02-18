package addrs_clustering_sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.lang.Math;
import java.io.*;

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
	
	private static double weightSum;
		
	public static void Partition(int clusterNum) {
		
		ArrayList<HashSet<Integer>> result = new ArrayList<HashSet<Integer>>();
		
		// first extract all edges/nodes from the graph to form connect table and compute weightSum.
		generateStructures();
		
		ArrayList<HashMap<Integer, Float>> remainsWeightToCluster = new ArrayList<HashMap<Integer, Float>>();
		
		// begin of the cluster partition
		for (int i = 0; i < clusterNum; ++i) {
			
			HashSet<Integer> cluster = new HashSet<Integer>();
			HashMap<Integer, Float> weightToCluster = new HashMap<Integer, Float>();
			double innerWeight = 0;
			double partitionWeight = 0;
			
			while (innerWeight + partitionWeight / 2 < weightSum / (clusterNum + 1) && edges.isEmpty() == false) {
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
				while (innerWeight + partitionWeight / 2 < weightSum / (clusterNum + 1) && weightToCluster.isEmpty() == false) {
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
			remainsWeightToCluster.add(weightToCluster);
			result.add(cluster);
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
			HashSet<Integer> newCluster = result.get(choice);
			newCluster.add(node);
			result.set(choice, newCluster);
		}
		
		try {
			File output = new File("./test/partitionResult.txt");
			output.createNewFile();
			try (FileWriter fw = new FileWriter(output);
			     BufferedWriter bw = new BufferedWriter(fw)
			) {	
				for (int i = 0; i < clusterNum; ++i) {
					HashSet<Integer> cluster = result.get(i);
					for (int node : cluster) {
						bw.write(((Integer)node).toString());
						bw.write(" ");
					}
					bw.newLine();
				}
				bw.flush();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	// generate edges, nodes and compute the weight sum of all edges in the same time
	private static void generateStructures() {
		connect_table = new HashMap<Integer, ArrayList<Integer>>();
		weight_table = new HashMap<Integer, ArrayList<Float>>();
		Graph graph = new Graph();
		graph.createGraph();
		HashMap<Integer,ArrayList<Float>> pair_table = graph.getEdgesInfos();
		edges = new HashMap<Long, Float>();
		nodes = new HashSet<Integer>();
		weightSum = 0;
		for (Map.Entry<Integer, ArrayList<Float>> entry : pair_table.entrySet()) {
			int thisEnd = entry.getKey();
			ArrayList<Float> pairs = entry.getValue();
			nodes.add(thisEnd);
			ArrayList<Integer> connections = new ArrayList<Integer>();
			ArrayList<Float> weights = new ArrayList<Float>();
			for (int i = 0; i < pairs.size(); i += 2) {
				int anotherEnd = pairs.get(i).intValue();
				float weight = pairs.get(i + 1);
				connections.add(anotherEnd);
				weights.add(weight);
				long edgeIndex = (((long)(Math.min(thisEnd, anotherEnd))) << 32)
						+ ((long)(Math.max(thisEnd, anotherEnd)));
				if (edges.containsKey(edgeIndex) == false) {
					edges.put(edgeIndex, weight);
					weightSum += weight;
				}
			}
			connect_table.put(thisEnd, connections);
			weight_table.put(thisEnd, weights);
		}
	}
}
