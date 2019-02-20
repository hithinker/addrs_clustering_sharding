

package com.btc.paper.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


//实验指标计算
public class Process {
	//流程:
	// 1.将所有边的is_updated字段更新为0(每次进行图分割结束时)，并将每个地址的cluster_id_new设置为-1(每次地址聚类前即可) 
	// 2.读入比特币交易数据，读入过程中给地址分配id,并更新历史图表
	// 3.图构造之前更新acc_weight(除第一次图构造前)
	// 4.边删除(除第一次图构造前)
	// 5.构造图
	// 6.将图传入图分割函数
	// 7.将得到的地址聚类结果存入数据库
	// 8.同步更新其他未参与地址聚类地址的新簇号(除第一次地址聚类外,每次分割后自动执行)
	// 9.实验指标统计：
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
