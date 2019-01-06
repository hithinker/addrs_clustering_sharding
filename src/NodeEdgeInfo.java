package addrs_clustering_sharding;

// NodeEdgeInfo represents a tuple of node index of another end of the edge
// and the edge weight. It's a relatively naive design but enough of use.
public class NodeEdgeInfo {
	private int anotherEnd;
	private float weight;
	public NodeEdgeInfo(int a, float w) {
		anotherEnd = a;
		weight = w;
	}
	public void setAnotherEnd(int a) {
		anotherEnd = a;
		return;
	}
	public int getAnotherEnd() {
		return anotherEnd;
	}
	public void setWeight(float w) {
		weight = w;
		return;
	}
	public float getWeight() {
		return weight;
	}
}
