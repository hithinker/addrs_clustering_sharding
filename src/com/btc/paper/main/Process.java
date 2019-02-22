

package com.btc.paper.main;

import java.util.HashMap;

import com.btc.paper.graph.GraphPartition;

//实验指标计算
public class Process {
	//流程:
	/* 1.读入所有的块数据，这时应该判断是否是首次运行(使用round来区分)程序 
	     a.如果是首次的问题，则直接分配一个addr-id的映射map,然后一边读数据，一边分配id,记得本次设置地址id计数器为0
	     b.如果不是首次，则读入地址分配id的文档,一边读入,一边分配id,同时注意这次的地址id计数器的初始值设置为计数器的大小
	     c.在读数据的过程中,以交易为单位,维护一个列表,记录每次出现的地址id
	     d.如果不是首次读入,要记得边读边判断UTXO或者地址所属的分片(或簇号)id,记得以交易为单位统计
	     e.上述流程完了以后可以clear掉addr-id映射map和id-cluster映射map
	   2.对上一步缓存的以交易为单位的边信息进行存储
	     a.对一个交易内的地址id进行边分配
	     b.同步更新历史图。更新方法，按行读取历史图后,查看在本次的增量边中是否有这行的边,如果有则更新边权重
	     c.对权重做一个判断,如果小于某个阈值则直接删除,如果大于某个阈值则加入到本次的增量更新图中,将不被删除的边写入更新的历史图文件中
	     d.对本周期内的边,未在历史图中出现的,直接写入历史图中去,写入历史图中
	   3.进行图分割产生地址-簇集
	   4.进行地址簇号映射的刷新
	     a.读入历史的地址簇号的映射关系文件,为了后边的更新，需要建立一个倒排索引,对于每个簇号建立一个Map,记录这个历史中在本簇中的地址本次分配中出现在
           哪个簇里了
         b.统计每个历史簇在本次分配中与哪个新簇关联关系最大,然后重新分配其余未参与分配的地址的新簇号	    
	
	*/
	public static void main(String[] args) {
		HashMap<Integer, Integer> results = GraphPartition.Partition(1024, 0);
		GraphPartition.freshClusters(results, 0, 2014);
	}
}
