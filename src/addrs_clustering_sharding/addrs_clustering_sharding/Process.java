package addrs_clustering_sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import DBUtil.DBOperation;
import data.BlockProcessor;

//ʵ��ָ�����
public class Process {
	//����:
	// 1.�����бߵ�is_updated�ֶθ���Ϊ0(ÿ�ν���ͼ�ָ����ʱ)������ÿ����ַ��cluster_id_new����Ϊ-1(ÿ�ε�ַ����ǰ����) 
	// 2.������رҽ������ݣ���������и���ַ����id,��������ʷͼ��
	// 3.ͼ����֮ǰ����acc_weight(����һ��ͼ����ǰ)
	// 4.��ɾ��(����һ��ͼ����ǰ)
	// 5.����ͼ
	// 6.��ͼ����ͼ�ָ��
	// 7.���õ��ĵ�ַ�������������ݿ�
	// 8.ͬ����������δ�����ַ�����ַ���´غ�(����һ�ε�ַ������,ÿ�ηָ���Զ�ִ��)
	// 9.ʵ��ָ��ͳ�ƣ�
	private static DBOperation dbOp = new DBOperation();
	private static BlockProcessor blkProcessor = new BlockProcessor(); 
	public static void main(String[] args) {
		dbOp.openConnection("jdbc:mysql://localhost:3306/btc_data?serverTimezone=UTC");
		dbOp.flushUpdated();
		dbOp.clusterIdPreProcess();
		try {
			blkProcessor.readBlock("/home/infosec/data0_999",dbOp,0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<HashSet<Integer>> clusters = GraphPartition.Partiton(1000);
		saveGraphPartition(clusters);
		dbOp.closeConnection();
	}
	public static void saveGraphPartition(ArrayList<HashSet<Integer>> clusters) {	
		for(int i=0;i<clusters.size();i++) {
			HashMap<Integer,Integer> idClusteridMap = new HashMap<Integer,Integer>();
			for(Integer id:clusters.get(i)) {
				idClusteridMap.put(id, i+1);
			}
			dbOp.updateClusterIdBatch(idClusteridMap);
		}
	}
}
