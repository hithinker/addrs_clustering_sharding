package com.btc.paper.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

public class BlockProcessor {
	public HashMap<int[],Float> readBlock(String dir, int round) throws Exception {
		File blockDir = new File(dir);
		StringBuilder sb = new StringBuilder();
		JSONObject block_json_obj = null;
		ArrayList<Integer> beforeClusteredShardStat = new ArrayList<Integer>();
		ArrayList<Integer> afterClusteredShardStat = new ArrayList<Integer>();
		HashMap<String,Integer> addr_id = new HashMap<String,Integer>();
		HashMap<Integer,ArrayList<Float>> epochEdges = new HashMap<Integer,ArrayList<Float>>();
		int idCounter = 0;
		int addrsCount = 0;
	    HashMap<Integer,Integer> id_cid = null;
		long start = System.currentTimeMillis();
		if(round > 0) {
			addr_id = this.getAddrIdMap("");
			addrsCount = addr_id.size();
			id_cid = new HashMap<Integer,Integer>();
		}
		LinkedList<HashSet<Integer>> idsList = new LinkedList<HashSet<Integer>>();
		for (File blockData : blockDir.listFiles()) {
			long bstart = System.currentTimeMillis();
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
				JSONObject block = blocksArr.getJSONObject(i);
				// 一个块里的交易
				JSONArray txs = block.getJSONArray("tx");
				int tx_size = txs.size();
				System.out.println("交易数:" + tx_size);
				for (int j = 0; j < tx_size; j++){					
				    HashSet<Integer> tx_ids = new HashSet<Integer>();
				    HashSet<Integer> randCid = new HashSet<Integer>();
				    HashSet<Integer> clusteredId = new HashSet<Integer>();
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
								if(!addr_id.containsKey(addr))
									addr_id.put(addr, addrsCount++);
								tx_ids.add(addr_id.get(addr));
								if(round > 0) {
									String utxo = prev_out.getString("tx_index") + " " + addr + prev_out.getString("value")
										+ prev_out.getString("script");
									int rcid = this.getRandShards(utxo, 10);
									randCid.add(rcid);
									if(id_cid.containsKey(addr_id.get(addr)))
										clusteredId.add(id_cid.get(addr_id.get(addr)));
									else
										clusteredId.add(rcid);
										 
								}							
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
							if(!addr_id.containsKey(addr))
								addr_id.put(addr, addrsCount++);
							tx_ids.add(addr_id.get(addr));
							if(round > 0) {
								String utxo = out.getString("tx_index") + " " + addr + out.getString("value")
									+ out.getString("script");
								int rcid = this.getRandShards(utxo, 10);
								randCid.add(rcid);
								if(id_cid.containsKey(addr_id.get(addr)))
									clusteredId.add(id_cid.get(addr_id.get(addr)));
								else
									clusteredId.add(rcid);
							}
						}
					}
					beforeClusteredShardStat.add(randCid.size());
					afterClusteredShardStat.add(clusteredId.size());
					idsList.add(tx_ids);
			 }					
	       }
			long bend = System.currentTimeMillis();
			System.out.println(bend-bstart);
		}		
		System.out.print("This round 参与交易的地址数为:" + addrsCount);
		//将地址-id映射关系持久化
		this.saveAddrIdMap(addr_id, "");
		addr_id.clear();
		id_cid.clear();
		HashMap<int[],Float> edge_weight = new HashMap<int[],Float>();
		for(Iterator<HashSet<Integer>> idsIterator = idsList.iterator();idsIterator.hasNext();) {
			HashSet<Integer> ids = idsIterator.next();
			idsIterator.remove();
			for(Iterator<Integer> idIterator = ids.iterator();idIterator.hasNext();) {
				int id = idIterator.next();
				idIterator.remove();
				for(int iid:ids) {
					int[] key = new int[2];
					int node1 = id;
					int node2 = iid;
					if(id > iid) {
						node1 = iid;
						node2 = id;
				    }
					key[0] = node1;
					key[1] = node2;
					if(edge_weight.containsKey(key))
						edge_weight.put(key, edge_weight.get(key) + 1);
					else
						edge_weight.put(key, (float) 1);
			}
		}
	}
		idsList.clear();
		HashMap<int[],Float> epochGraph = new HashMap<int[],Float>(); 
		//更新历史图
		if(round > 0) {
			BufferedReader ebr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(" "))));
			BufferedWriter ebw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(" "))));
		    HashMap<int[],Float> remainingEdges = new HashMap<int[],Float>();
		    String edgeInfo = null;
		    int edgeCounter = 0;
		    while((edgeInfo = ebr.readLine()) != null) {
		    	edgeCounter++;
		    	String[] nodeWeight = edgeInfo.trim().split(" ");
		    	int node1 = Integer.parseInt(nodeWeight[0]);
		    	int node2 = Integer.parseInt(nodeWeight[1]);
		    	float weight = Integer.parseInt(nodeWeight[2]);
		    	int[] edge = new int[] {node1,node2};
		    	if(edge_weight.containsKey(edge)) {
		    		weight = (float) Math.pow(Math.pow(weight, 0.75)+edge_weight.get(edge),0.75);
		    		epochGraph.put(edge, weight);
		    		edge_weight.remove(edge);
		    	}else {
		    		remainingEdges.put(edge, (float) (weight*0.75));
		    	}
		    	if(edgeCounter >= 10000000) {
		    		for(int[] e:epochGraph.keySet()) {
		    			float w = epochGraph.get(e);
		    			String new_line = e[0] + " " + e[1] + " " + w;
		    			ebw.write(new_line);
		    		}
		    		epochGraph.clear();
		    		for(int[] e:remainingEdges.keySet()) {
		    			float w = remainingEdges.get(e);
		    			String new_line = e[0] + " " + e[1] + " " + w;
		    			ebw.write(new_line);
		    		}
		    		remainingEdges.clear();
		    		edgeCounter = 0;
		    	}
		    }
		    epochGraph.putAll(edge_weight);
		}else
			epochGraph = edge_weight;
		long end = System.currentTimeMillis();
		System.out.println("round" + round + "读入数据共用时:(millsecond)" + (end - start));
		return epochGraph;
	}
	//读取地址-id的映射关系，便于分配地址id
	public HashMap<String,Integer> getAddrIdMap(String idCounterPath){
		HashMap<String,Integer> addr_id = new HashMap<String,Integer>();
		try {
			FileInputStream fis= new FileInputStream(idCounterPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while((line = br.readLine()) != null) {
				String[] pair = line.split(" ");
				String addr = pair[0];
				int id = Integer.parseInt(pair[0]);
				addr_id.put(addr,id);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(IOException e ) {
			e.printStackTrace();
		}
		return addr_id;
	}
	public HashMap<Integer,Integer> getIdCid(String idCidPath){
		HashMap<Integer,Integer> id_cid = new  HashMap<Integer,Integer>();
		try {
			FileInputStream fis= new FileInputStream(idCidPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while((line = br.readLine()) != null) {
				String[] pair = line.split(" ");
				int id = Integer.parseInt(pair[0]);
				int cid = Integer.parseInt(pair[0]);
				id_cid.put(id,cid);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(IOException e ) {
			e.printStackTrace();
		}
		return id_cid;
	}
	//addr-id持久化
	public void saveAddrIdMap(HashMap<String,Integer> addrIdMap,String addrIdPath){
		try {
			FileOutputStream fos= new FileOutputStream(new File(addrIdPath));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			for(String addr:addrIdMap.keySet()) {
				String line = addr + " " + addrIdMap.get(addr);
				bw.write(line);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		}
		
	}
	//保存历史图
	public void saveHistoryGraph(HashMap<int[],Integer> epochEdges,HashMap<int[],Integer> remainingEdges,int round) {
		
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