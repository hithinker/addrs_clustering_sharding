package addrs_clustering_sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.lang.Math;

// GraphPartition implements the function of partitioning a graph to multiple
// clusters. Input of Partition is a hash map representing the connect table,
// and the number of clusters.
public class GraphPartition {
	private static HashMap<Integer, ArrayList<NodeEdgeInfo>> connect_table;
	
	// here a long represents two integer index of nodes appending by ascending order
	private static HashMap<Long, Float> edges;
	private static HashSet<Integer> nodes;
	
	private static double weightSum;
		
	public static ArrayList<HashSet<Integer>> Partiton(HashMap<Integer, ArrayList<NodeEdgeInfo>> graph, int clusterNum) {
		connect_table = graph;
		ArrayList<HashSet<Integer>> result = new ArrayList<HashSet<Integer>>();
		
		// first extract all edges/nodes from the connect table and compute weightSum.
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
				double weight = 0;
				for (Map.Entry<Long, Float> entry : edges.entrySet()) {
					if (entry.getValue() > weight) {
						weight = entry.getValue();
						startNodes = entry.getKey();
					}
				}
				innerWeight += weight;
				edges.remove(startNodes);
				
				// add the first two nodes to cluster
				int node1 = (int)(startNodes >> 32);
				int node2 = (int)startNodes;
				nodes.remove(node1);
				nodes.remove(node2);
				cluster.add(node1);
				cluster.add(node2);
				for (NodeEdgeInfo edge : connect_table.get(node1)) {
					int another = edge.getAnotherEnd();
					if (cluster.contains(another) == false) {
						partitionWeight += edge.getWeight();
						if (nodes.contains(another)) {
							long edgeIndex = ((long)(Math.min(node1, another))) << 32
									+ ((long)(Math.max(node1, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + edge.getWeight());
							else
								weightToCluster.put(another, edge.getWeight());
						}
					}
				}
				for (NodeEdgeInfo edge : connect_table.get(node2)) {
					int another = edge.getAnotherEnd();
					if (cluster.contains(another) == false) {
						partitionWeight += edge.getWeight();
						if (nodes.contains(another)) {
							long edgeIndex = ((long)(Math.min(node2, another))) << 32
									+ ((long)(Math.max(node2, another)));
							edges.remove(edgeIndex);
							if (weightToCluster.containsKey(another))
								weightToCluster.put(another, weightToCluster.get(another) + edge.getWeight());
							else
								weightToCluster.put(another, edge.getWeight());
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
					for (NodeEdgeInfo edge : connect_table.get(newNode)) {
						int another = edge.getAnotherEnd();
						if (cluster.contains(another) == false) {
							partitionWeight += edge.getWeight();
							if (nodes.contains(another)) {
								long edgeIndex = ((long)(Math.min(newNode, another))) << 32
										+ ((long)(Math.max(newNode, another)));
								edges.remove(edgeIndex);
								if (weightToCluster.containsKey(another))
									weightToCluster.put(another, weightToCluster.get(another) + edge.getWeight());
								else
									weightToCluster.put(another, edge.getWeight());
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
				if (remainsWeightToCluster.get(i).get(node) > weight) {
					weight = remainsWeightToCluster.get(i).get(node);
					choice = i;
				}
			}
			HashSet<Integer> newCluster = result.get(choice);
			newCluster.add(node);
			result.set(choice, newCluster);
		}
		
		return result;
	}
	
	// generate edges, nodes and compute the weight sum of all edges in the same time
	private static void generateStructures() {
		edges = new HashMap<Long, Float>();
		nodes = new HashSet<Integer>();
		weightSum = 0;
		for (Map.Entry<Integer, ArrayList<NodeEdgeInfo>> entry : connect_table.entrySet()) {
			nodes.add(entry.getKey());
			for (NodeEdgeInfo edge : entry.getValue()) {
				long edgeIndex = ((long)(Math.min(entry.getKey(), edge.getAnotherEnd()))) << 32
						+ ((long)(Math.max(entry.getKey(), edge.getAnotherEnd())));
				if (edges.containsKey(edgeIndex) == false) {
					edges.put(edgeIndex, edge.getWeight());
					weightSum += edge.getWeight();
				}
			}
		}
	}
}
