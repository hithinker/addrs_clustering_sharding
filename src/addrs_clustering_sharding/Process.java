package addrs_clustering_sharding;

import java.util.ArrayList;
import java.util.HashMap;

import DBUtil.DBOperation;

//实验指标计算
public class Process {
	public static void main(String[] args) {
	DBOperation dbOp = new DBOperation();
	dbOp.openConnection("jdbc:mysql://localhost:3306/btc_data?serverTimezone=UTC");
	ArrayList<String> addrs = new ArrayList<String>();
	addrs.add("111");
	addrs.add("7777");
	ArrayList<Integer> ids = new ArrayList<Integer>();
	ids.add(1);
	ids.add(2);
	ids.add(3);
	dbOp.updateClusterIdByAddr("7777", 1);
	HashMap<Integer,Integer> idClusterIdMap = new HashMap<Integer,Integer>();
	ArrayList<String> edges = new ArrayList<String>();
	edges.add("1,2");
	edges.add("1,3");
	edges.add("2,3");
	//dbOp.updateUnClusteredId(3);
	System.out.println(dbOp.getEpochEdges(1));
	}
}
