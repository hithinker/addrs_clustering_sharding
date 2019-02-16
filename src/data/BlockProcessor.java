package data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import DBUtil.DBOperation;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

public class BlockProcessor {
	public void readBlock(String dir, DBOperation dbOp, int round) throws Exception {
		File blockDir = new File(dir);
		StringBuilder sb = new StringBuilder();
		JSONObject block_json_obj = null;
		ArrayList<Integer> beforeClusteredShardStat = new ArrayList<Integer>();
		ArrayList<Integer> afterClusteredShardStat = new ArrayList<Integer>();
		//注意当round>0时存储的是utxo
		LinkedList<ArrayList<String>> round_tx_addrs = new LinkedList<ArrayList<String>>();; 
		HashSet<String> round_addrs = new HashSet<String>();
		int addrsCount = 0;
		for (File blockData : blockDir.listFiles()) {
			String line = null;
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(blockData)));
				while ((line = br.readLine()) != null)
					sb.append(line);
				block_json_obj = JSON.parseObject(sb.toString());
				sb.setLength(0);
			} catch (FileNotFoundException e) {
				System.out.println(blockData.getName() + "is not found!!");
			} catch (IOException ioe) {
				System.out.println(blockData.getName() + "reading error!!");
			}
			JSONArray blocksArr = block_json_obj.getJSONArray("blocks");
			int blocks_size = blocksArr.size();
			for (int i = 0; i < blocks_size; i++) {
				long start = System.currentTimeMillis();
				JSONObject block = blocksArr.getJSONObject(i);
				// 一个块里的交易
				JSONArray txs = block.getJSONArray("tx");
				int tx_size = txs.size();
				System.out.println("round " + round + "共有交易:" + tx_size);//打印交易数
				for (int j = 0; j < tx_size; j++){					
					ArrayList<String> tx_addrs = new ArrayList<String>(); 
					JSONObject tx = txs.getJSONObject(j);
					// 输入部分的UTXO
					JSONArray inputs = tx.getJSONArray("inputs");
					int input_size = inputs.size();
					for (int k = 0; k < input_size; k++) {
						JSONObject input = inputs.getJSONObject(k);
						JSONObject prev_out = input.getJSONObject("prev_out");
						if (prev_out != null) {
							String addr = prev_out.getString("addr");
							if (addr != null) {
								addrsCount++;
								round_addrs.add(addr);
								if(round > 0) {
									String utxo = prev_out.getString("tx_index") + " " + addr + prev_out.getString("value")
										+ prev_out.getString("script");
									tx_addrs.add(utxo);
								}else
									tx_addrs.add(addr);
								
							}
						}
					}
					// 输出部分的UTXO
					JSONArray outs = tx.getJSONArray("out");
					int outSize = outs.size();
					for (int k = 0; k < outSize; k++) {
						JSONObject out = outs.getJSONObject(k);
						String addr = out.getString("addr");
						if (addr != null) {
							addrsCount++;
							round_addrs.add(addr);
							if(round > 0) {
								String utxo = out.getString("tx_index") + " " + addr + out.getString("value")
									+ out.getString("script");
								tx_addrs.add(utxo);
							}else {
								tx_addrs.add(addr);
							}
						}
					}
					round_tx_addrs.add(tx_addrs);
					System.out.print("This round 参与交易的地址数为:" + addrsCount);
					System.out.print("This round 共有不重复地址数为:" + round_addrs.size());
					dbOp.allocateAddrsIdsBatch(round_addrs);
					round_addrs.clear();
					//先进行实验测试本次的跨片交易数的对比情况
					HashMap<String,Integer> addr_cid = dbOp.getAddrCidByIsUpdate();
					if(round > 0) {
						for(ArrayList<String> addrsInTx:round_tx_addrs) {
							HashSet<Integer> prtnClusters = new HashSet<Integer>();
							HashSet<Integer> randClusters = new HashSet<Integer>();
							for(String utxo:addrsInTx) {
								int index = utxo.indexOf(" ");
								String addr=utxo.substring(index + 1,index + 35);
								int randCid = getRandShards(utxo,10);
								randClusters.add(randCid);
								if(addr_cid.containsKey(addr))
									prtnClusters.add(addr_cid.get(addr));
								else
									prtnClusters.add(randCid);
							}
							beforeClusteredShardStat.add(randClusters.size());
							afterClusteredShardStat.add(prtnClusters.size());
						}
					}
					addr_cid.clear();
					// 将边存入历史图
					HashMap<String, Integer> addrIdMap = dbOp.getAddrIdByIsUpdate();
					ArrayList<String> edges = new ArrayList<String>();
					int edgeCount = 0;
					for (Iterator<ArrayList<String>> iterator = round_tx_addrs.iterator();iterator.hasNext();) {
						ArrayList<String> addrsInATx = iterator.next();
						ArrayList<Integer> addrIds = new ArrayList<Integer>();
						for(String addr:addrsInATx)
							addrIds.add(addrIdMap.get(addr));					
					    for (int k = 0; k < addrIds.size(); k++)
					    	for (int l = k + 1; l < addrIds.size(); l++) {
					    		if (addrIds.get(k) > addrIds.get(l))
					    			edges.add(addrIds.get(l) + "," + addrIds.get(k));
					    		else
					    			edges.add(addrIds.get(k) + "," + addrIds.get(l));
					    		edgeCount++;
					    	}	
					    iterator.remove();
					}
					dbOp.insertEdgesBatch(edges);
					addrIdMap.clear();
				    System.out.println("round " + round + "共有边数:" + edgeCount);
				long end = System.currentTimeMillis();
				System.out.println(end - start);
			}
					
	       }
		}
	}
	// 细粒度的统计：是以块为单位的
	// 但这里我们以单个交易额UTXO为单位进行统计可以减少内存的UTXO存储，
	public Short getRandShards(String utxo, int bitCount) {
			byte[] byteBuffer = null;
			try {
				MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
				messageDigest.update(utxo.getBytes());
				byteBuffer = messageDigest.digest();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			int j = 0;
			StringBuilder sb = new StringBuilder();
			while (bitCount > 0) {
				if ((bitCount / 8) > 0) {
					sb.append(getBitString(byteBuffer[j], 8));
					j++;
				} else {
					sb.append(getBitString(byteBuffer[j], bitCount));
				}
				bitCount -= 8;
			}
			short order = Short.valueOf(sb.toString(), 2);
		return order;
	}
	public String getBitString(byte b, int count) {
		byte operator = 1;
		StringBuilder sb = new StringBuilder();
		for (int i = 7; i >= 8 - count; i--) {
			byte result = (byte) ((b >>> i) & operator);
			sb.append(result == 0 ? "0" : "1");

		}
		return sb.toString();
	}
}