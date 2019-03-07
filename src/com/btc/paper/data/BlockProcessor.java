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
	public HashMap<Integer, ArrayList<Float>> readBlock(String dir, int round) {
		File blockDir = new File(dir);
		StringBuilder sb = new StringBuilder();
		JSONObject block_json_obj = null;
		// 划分地址簇前后对比
		ArrayList<Integer> beforeClusteredShardStat = new ArrayList<Integer>();
		ArrayList<Integer> afterClusteredShardStat = new ArrayList<Integer>();
		int randShardingCount = 0;
		int clusteredShardingCount = 0;
		// addr-id Map
		HashMap<String, Integer> addr_id = new HashMap<String, Integer>();
		// id-cid 映射关系图
		HashMap<Integer, Integer> id_cid = null;
		int addrsCount = 0;
		int epochAddrCount = 0;
		int txsCount = 0;
		// start 时间
		long start = System.currentTimeMillis();
		if (round > 0) {
			addr_id = this.getAddrIdMap("/home/infosec/sharding_expt/addrid.txt");
			addrsCount = addr_id.size();
			id_cid = this.getIdCid("/home/infosec/sharding_expt/idCid" + (round - 1) + ".txt");
		}
		// 注意是LinkedList,记录每个交易内的地址
		LinkedList<HashSet<Integer>> idsList = new LinkedList<HashSet<Integer>>();
		HashSet<Integer> targetedIds = new HashSet<Integer>();
		for (File blockData : blockDir.listFiles()) {
			String line = null;
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(blockData)));
				while ((line = br.readLine()) != null)
					sb.append(line);
				block_json_obj = JSON.parseObject(sb.toString());
				sb.setLength(0);
			} catch (FileNotFoundException e) {
				System.out.println(blockData.getName() + "is not found!!");
			} catch (IOException ioe) {
				System.out.println(blockData.getName() + "reading error!!");
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			JSONArray blocksArr = block_json_obj.getJSONArray("blocks");
			int blocks_size = blocksArr.size();
			for (int i = 0; i < blocks_size; i++) {
				JSONObject block = blocksArr.getJSONObject(i);
				// 一个块里的交易
				JSONArray txs = block.getJSONArray("tx");
				int tx_size = txs.size();
				txsCount += tx_size;
				for (int j = 0; j < tx_size; j++) {
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
								if (!addr_id.containsKey(addr)) {
									addr_id.put(addr, addrsCount++);
									epochAddrCount++;
								}
								tx_ids.add(addr_id.get(addr));
								if (round > 0) {
									if (id_cid.containsKey(addr_id.get(addr))) {
										targetedIds.add(addr_id.get(addr));
										String utxo = prev_out.getString("tx_index") + " " + addr
												+ prev_out.getString("value") + prev_out.getString("script");
										int rcid = this.getRandShards(utxo, 10);
										randCid.add(rcid);
										clusteredId.add(id_cid.get(addr_id.get(addr)));
									}
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
							if (!addr_id.containsKey(addr)) {
								addr_id.put(addr, addrsCount++);
								epochAddrCount++;
							}
							tx_ids.add(addr_id.get(addr));
							if (round > 0) {
								if (id_cid.containsKey(addr_id.get(addr))) {
									targetedIds.add(addr_id.get(addr));
									String utxo = out.getString("tx_index") + " " + addr + out.getString("value")
											+ out.getString("script");
									int rcid = this.getRandShards(utxo, 10);
									randCid.add(rcid);
									clusteredId.add(id_cid.get(addr_id.get(addr)));
								}
							}
						}
					}
					beforeClusteredShardStat.add(randCid.size());
					afterClusteredShardStat.add(clusteredId.size());
					idsList.add(tx_ids);
				}
			}
		}
		System.out.println("round" + round + "参与交易总数为:" + txsCount);
		System.out.println("round" + round + "参与交易的新地址(本轮中被分配地址)数为:" + epochAddrCount);
		System.out.println("round" + round + "参与交易的旧地址数为:" + targetedIds.size());
		System.out.println("截至 round" + round + "总的地指数为:" + addr_id.size());
		// 将addr-id映射关系持久化
		this.saveAddrIdMap(addr_id, "/home/infosec/sharding_expt/addrid.txt");
		addr_id.clear();
		if (round > 0)
			id_cid.clear();
		// 计算本周期的增量边
		HashMap<Integer, HashMap<Integer, Float>> edge_weight = new HashMap<Integer, HashMap<Integer, Float>>();
		for (Iterator<HashSet<Integer>> idsIterator = idsList.iterator(); idsIterator.hasNext();) {
			HashSet<Integer> ids = idsIterator.next();
			idsIterator.remove();
			for (Iterator<Integer> idIterator = ids.iterator(); idIterator.hasNext();) {
				int id = idIterator.next();
				idIterator.remove();
				for (int iid : ids) {
					int node1 = id;
					int node2 = iid;
					if (id > iid) {
						node1 = iid;
						node2 = id;
					}
					if (edge_weight.containsKey(node1))
						if (edge_weight.get(node1).containsKey(node2))
							edge_weight.get(node1).put(node2, edge_weight.get(node1).get(node2) + 1);
						else
							edge_weight.get(node1).put(node2, (float) 1);
					else {
						HashMap<Integer, Float> singleModeMap = new HashMap<Integer, Float>();
						singleModeMap.put(node2, (float) 1);
						edge_weight.put(node1, singleModeMap);
					}
				}
			}
		}
		idsList.clear();
		int edgeCount = 0;
		for (int endPoint : edge_weight.keySet())
			edgeCount += (edge_weight.get(endPoint).size());
		System.out.println("round" + round + "共有边数" + edgeCount);
		HashMap<Integer, ArrayList<Float>> epochGraph = new HashMap<Integer, ArrayList<Float>>();		
		if (round > 0) {
			//以下为计算跨片结果
			for (int i = 0; i < beforeClusteredShardStat.size(); i++) {
				randShardingCount += beforeClusteredShardStat.get(i);
			}
			for (int i = 0; i < afterClusteredShardStat.size(); i++) {
				clusteredShardingCount += afterClusteredShardStat.get(i);
			}
			System.out.println("round" + round + "在未地址聚类前的跨片数为:" + randShardingCount);
			System.out.println("round" + round + "地址聚类后的跨片数为:" + clusteredShardingCount);
			//更新历史图
			BufferedReader ebr = null;
			BufferedWriter ebw = null;
			try {
				File preEdgeFile = new File("/home/infosec/sharding_expt/" + "edges" + (round - 1) + ".txt");
				File newEdgeFile = new File("/home/infosec/sharding_expt/edges" + round + ".txt");
				ebr = new BufferedReader(new InputStreamReader(new FileInputStream(preEdgeFile)));
				ebw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newEdgeFile)));
				HashMap<Integer, HashMap<Integer, Float>> remainingEdges = new HashMap<Integer, HashMap<Integer, Float>>();
				String edgeInfo = null;
				int edgeCounter = 0;
				int historyGraphEdgeCounter=0;
				while ((edgeInfo = ebr.readLine()) != null) {
					if (edgeInfo.trim().length() < 1)
						break;
					edgeCounter++;
					historyGraphEdgeCounter++;
					String[] nodeWeight = edgeInfo.trim().split(" ");
					int node1 = Integer.parseInt(nodeWeight[0]);
					int node2 = Integer.parseInt(nodeWeight[1]);
					float weight = Float.parseFloat(nodeWeight[2]);
					if (edge_weight.containsKey(node1) && edge_weight.get(node1).containsKey(node2)) {						   
						weight = (float) Math.pow(Math.pow(weight, 0.75) + edge_weight.get(node1).get(node2), 0.75);
						// 删除条件>1 if (weight > 1)
							if (epochGraph.containsKey(node1)) {
								epochGraph.get(node1).add((float) node2);
								epochGraph.get(node1).add(weight);
							}
							else {
								ArrayList<Float> singleNodeAdjList = new ArrayList<Float>();
								singleNodeAdjList.add((float) node2);
								singleNodeAdjList.add(weight);
								epochGraph.put(node1, singleNodeAdjList);
							}
							if (epochGraph.containsKey(node2)) {
								epochGraph.get(node2).add((float) node1);
								epochGraph.get(node2).add(weight);
							}
							else {
								ArrayList<Float> singleNodeAdjList = new ArrayList<Float>();
								singleNodeAdjList.add((float) node1);
								singleNodeAdjList.add(weight);
								epochGraph.put(node2, singleNodeAdjList);
							}
							/*
						else if (remainingEdges.containsKey(node1))
							edge_weight.get(node1).put(node2, weight);
						else {
							HashMap<Integer, Float> singleNodeMap = new HashMap<Integer, Float>();
							singleNodeMap.put(node2, weight);
							remainingEdges.put(node1, singleNodeMap);
						}
                         */
						edge_weight.get(node1).remove(node2);
					} else {
						float weightt = (float) (weight * 0.75);
						if (weightt > 0.5)
							if (remainingEdges.containsKey(node1))
								remainingEdges.get(node1).put(node2, weightt);
							else {
								HashMap<Integer, Float> singleNodeMap = new HashMap<Integer, Float>();
								singleNodeMap.put(node2, weightt);
								remainingEdges.put(node1, singleNodeMap);
							}
					}
					if (edgeCounter >= 10000000) {
						for (int node : epochGraph.keySet()) {
							ArrayList<Float> adjList= epochGraph.get(node);
							int i = 0;
							while (i < adjList.size()) {
								String new_line = node + " " + (int)adjList.get(i++).floatValue() + " " + adjList.get(i++)
										+ "\n";
								ebw.write(new_line);
							}
						}
						for (int node : remainingEdges.keySet()) {
							HashMap<Integer, Float> singleNodeMap = remainingEdges.get(node);
							for (int anotherNode : singleNodeMap.keySet()) {
								String new_line = node + " " + anotherNode + " " + singleNodeMap.get(anotherNode)
										+ "\n";
								ebw.write(new_line);
							}
						}
						remainingEdges.clear();
						edgeCounter = 0;
					}
				}
				preEdgeFile.delete();
				for (int node : edge_weight.keySet()) {
					HashMap<Integer, Float> adjList = edge_weight.get(node);
					for (int adjNode : adjList.keySet()) {
						float weight = adjList.get(adjNode);
						String line = node + " " + adjNode + " " + weight + "\n";
						ebw.write(line);
						historyGraphEdgeCounter++;
						// 删除条件>1 if (weight > 1)
						if (epochGraph.containsKey(node)) {
							epochGraph.get(node).add((float) adjNode);
							epochGraph.get(node).add(weight);
						}
						else {
							ArrayList<Float> singleNodeAdjList = new ArrayList<Float>();
							singleNodeAdjList.add((float) node);
							singleNodeAdjList.add(weight);
							epochGraph.put(node, singleNodeAdjList);
						}
						if (epochGraph.containsKey(adjNode)) {
							epochGraph.get(adjNode).add((float) node);
							epochGraph.get(adjNode).add(weight);
						}
						else {
							ArrayList<Float> singleNodeAdjList = new ArrayList<Float>();
							singleNodeAdjList.add((float) adjNode);
							singleNodeAdjList.add(weight);
							epochGraph.put(adjNode, singleNodeAdjList);
						}
					}
				}
				System.out.println("历史图总边数为:" + historyGraphEdgeCounter);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (ebr != null)
						ebr.close();
					if (ebw != null)
						ebw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				;
			}
		
		} else {
			for (int node : edge_weight.keySet()) {
				HashMap<Integer, Float> adjList = edge_weight.get(node);
				for (int adjNode : adjList.keySet()) {
					float weight = adjList.get(adjNode);
					// 删除条件>1 if (weight > 1)
					if (epochGraph.containsKey(node)) {
						epochGraph.get(node).add((float) adjNode);
						epochGraph.get(node).add(weight);
					}
					else {
						ArrayList<Float> singleNodeAdjList = new ArrayList<Float>();
						singleNodeAdjList.add((float) adjNode);
						singleNodeAdjList.add(weight);
						epochGraph.put(node, singleNodeAdjList);
					}
					if (epochGraph.containsKey(adjNode)) {
						epochGraph.get(adjNode).add((float) node);
						epochGraph.get(adjNode).add(weight);
					}
					else {
						ArrayList<Float> singleNodeAdjList = new ArrayList<Float>();
						singleNodeAdjList.add((float) node);
						singleNodeAdjList.add(weight);
						epochGraph.put(adjNode, singleNodeAdjList);
					}
				}
			}
			this.saveInitialEdges(edge_weight);
		}		
		long end = System.currentTimeMillis();
		System.out.println("round" + round + "读入数据共用时:(millsecond)" + (end - start));
		return epochGraph;
	}
	//读取地址-id的映射关系，便于分配地址id
	private HashMap<String,Integer> getAddrIdMap(String idCounterPath){
		HashMap<String,Integer> addr_id = new HashMap<String,Integer>();
		FileInputStream fis = null;
		BufferedReader br = null;
		try {
			File idFile = new File(idCounterPath);
			fis= new FileInputStream(idFile);
			br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while((line = br.readLine()) != null) {
				if(line.trim().length() < 1)
					break;
				String[] pair = line.trim().split(" ");
				String addr = pair[0];
				int id = Integer.parseInt(pair[1]);
				addr_id.put(addr,id);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(IOException e ) {
			e.printStackTrace();
		}finally {			
			try {
				if(br != null)
					br.close();
				if(fis!=null)
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
		}
		return addr_id;
	}
	private HashMap<Integer,Integer> getIdCid(String idCidPath){
		HashMap<Integer,Integer> id_cid = new  HashMap<Integer,Integer>();
		FileInputStream fis = null;
		BufferedReader br = null;
		try {
			File idCidFile = new File(idCidPath);
			System.out.println(idCidFile.lastModified());
			fis= new FileInputStream(idCidPath);
			br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while((line = br.readLine()) != null) {
				if(line.trim().length() < 1)
					break;
				String[] pair = line.trim().split(" ");
				int id = Integer.parseInt(pair[0]);
				int cid = Integer.parseInt(pair[1]);
				id_cid.put(id,cid);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(IOException e ) {
			e.printStackTrace();
		}finally {			
			try {
				if(br != null)
					br.close();
				if(fis!=null)
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
		}
		return id_cid;
	}
	//addr-id持久化
	public void saveAddrIdMap(HashMap<String,Integer> addrIdMap,String addrIdPath){
		File addrIdFile = new File(addrIdPath);	
		FileOutputStream fos = null;
		BufferedWriter bw = null;
		try {
			if(!addrIdFile.exists())
				addrIdFile.createNewFile();
			fos= new FileOutputStream(addrIdFile);
			bw = new BufferedWriter(new OutputStreamWriter(fos));
			for(String addr:addrIdMap.keySet()) {
				String line = addr + " " + addrIdMap.get(addr) + "\n";
				bw.write(line);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		}finally {			
				try {
					if(bw != null)
						bw.close();
					if(fos != null)
						fos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}			
		}
		
	}
	public void saveInitialEdges(HashMap<Integer, HashMap<Integer, Float>> edge_weight) {
		File edgesFile = new File("/home/infosec/sharding_expt/edges0.txt");
		BufferedWriter bw = null;
		try {
			if(!edgesFile.exists()) {
				edgesFile.createNewFile();
			}
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(edgesFile)));
			for(int node:edge_weight.keySet()) {
				HashMap<Integer,Float> adjList = edge_weight.get(node);
				for(int anotherNode:adjList.keySet()) {
					float weight = adjList.get(anotherNode);
				    String edge = node + " " + anotherNode + " " + weight + "\n";
				    bw.write(edge);
				}
			}
		}catch(IOException e) {
			e.printStackTrace();
		}finally {
				try {
					if(bw != null)
						bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
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