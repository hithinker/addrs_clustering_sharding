

package com.btc.paper.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


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
	public static void main(String[] args) {
		
	}
	public static void saveGraphPartition(ArrayList<HashSet<Integer>> clusters) {	
		for(int i=0;i<clusters.size();i++) {
			HashMap<Integer,Integer> idClusteridMap = new HashMap<Integer,Integer>();
			for(Integer id:clusters.get(i)) {
				idClusteridMap.put(id, i+1);
			}
			
		}
	}
}
