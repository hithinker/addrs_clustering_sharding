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


public class BlockProcessor {
    DBOperation dbOp = new DBOperation();
    public void readBlock(String dir,int round) throws Exception{
        File blockDir = new File(dir);
        StringBuilder sb = new StringBuilder();
        JSONObject block_json_obj = null;
        ArrayList<Integer> beforeClusteredShardStat= new ArrayList<Integer>();
        ArrayList<Integer> afetrClusteredShardStat= new ArrayList<Integer>();
        dbOp.openConnection("mysql:jdbc://127.0.0.1:3306/btc_data?user=root&password=adminmzl&autoReconnect=true");
        for(File blockData:blockDir.listFiles()){
            String line = null;
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(blockData)));
                while((line = br.readLine()) != null)
                    sb.append(line);
                block_json_obj = JSON.parseObject(sb.toString());
                sb.setLength(0);
            }catch(FileNotFoundException e){
                System.out.println(blockData.getName() + "is not found!!");
            }catch(IOException ioe){
                System.out.println(blockData.getName() + "reading error!!");
            }
            JSONArray blocksArr = block_json_obj.getJSONArray("blocks");
            int blocks_size = blocksArr.size();
            for(int i = 0;i<blocks_size;i++){
                JSONObject block = blocksArr.getJSONObject(i);
                //一个块里的交易
                JSONArray txs= block.getJSONArray("tx");
                int tx_size = txs.size();
                //System.out.println(tx_size);//打印交易数
                for(int j = 0;j < tx_size;j++){
                    ArrayList<String> utxoes = new ArrayList<String>();
                    ArrayList<String> addrs = new ArrayList<String>();
                    JSONObject tx = txs.getJSONObject(j);
                    //输入部分的UTXO
                    JSONArray inputs = tx.getJSONArray("inputs");
                    int input_size = inputs.size();
                    for(int k = 0;k < input_size;k++){
                        JSONObject input = inputs.getJSONObject(k);
                        JSONObject prev_out = input.getJSONObject("prev_out");
                        if(prev_out != null){
                            String addr = prev_out.getString("addr");
                            if(addr != null) {
                            addrs.add(addr);
                            utxoes.add(prev_out.getString("tx_index")+addr+prev_out.getString("value")+prev_out.getString("script"));
                        }
                      }
                   }
                    //输出部分的UTXO
                    JSONArray outs = tx.getJSONArray("out");
                    int outSize = outs.size();
                    for(int k = 0;k<outSize;k++){
                        JSONObject out = outs.getJSONObject(k);
                        String addr = out.getString("addr");
                        if(addr != null){
                            addrs.add(addr);
                            utxoes.add(out.getString("tx_index")+addr+out.getString("value")+out.getString("script"));
                        }
                    }
                    //先给地址分配id
                    dbOp.allocateAddrsIdsBatch(addrs);
                    //将边存入历史图
                    HashMap<String,Integer> addrIdMap = dbOp.getAddrIdMap(addrs);
                    ArrayList<Integer> idsByOrder = new ArrayList<Integer>();
                    for(String addr:addrs) {
                    	idsByOrder.add(addrIdMap.get(addr));
                    }
                    ArrayList<Integer> ids = new ArrayList<Integer>(addrIdMap.values());
                    ArrayList<String>  edges= new ArrayList<String>();
                    for(int k = 0;k<ids.size();k++) {
                    	for(int l = k+1;k<ids.size();l++) {
                    		edges.add(ids.get(k) + "," +ids.get(l));
                    			}
                    		}
                    dbOp.insertEdgesBatch(edges);
                  //判断当前的epoch轮次(round),如果大于1则是进行过了地址图分割了,如果有则进行实验指标判断
                    //先是UTXO随机性判断
                    //看看地址聚类后的情况
                   if(round>0) {
                	   ArrayList<Short> shardList = this.getRandShards(utxoes, 10);
                	   beforeClusteredShardStat.add(new HashSet<Short>(shardList).size());
                	   HashMap<Integer,Integer> idClusterIdMap = dbOp.getIdClusterIdMap(ids);
                	   HashSet<Integer> clusterIdSet = new HashSet<Integer>();
                	   for(int n = 0;n<idsByOrder.size();n++) {
                		   if(idClusterIdMap.get(idsByOrder.get(i)) != -1)
                			   clusterIdSet.add(idClusterIdMap.get(idsByOrder.get(i)));
                		   else
                			   clusterIdSet.add(shardList.get(i).intValue());
                	   }
                	   afetrClusteredShardStat.add(clusterIdSet.size());               	   
                   }
                    	}
                    }                                   
        }
        dbOp.closeConnection();
        //计算地址聚类以后的分片数
        int afterShardsCount = 0;
        for(int i = 0;i<afetrClusteredShardStat.size();i++)
        	afterShardsCount += afetrClusteredShardStat.get(i);
        int beforeShardsCount = 0;
        for(int i = 0;i<beforeClusteredShardStat.size();i++)
        	beforeShardsCount += beforeClusteredShardStat.get(i);
        //打印计算结果
        System.out.println("before:"+beforeShardsCount+",after:"+afterShardsCount+",decrease:"+(beforeShardsCount-afterShardsCount)/beforeShardsCount);
    }
    //细粒度的统计：是以块为单位的
    //但这里我们以单个交易额UTXO为单位进行统计可以减少内存的UTXO存储，
    public ArrayList<Short> getRandShards(ArrayList<String> utxoes,int bitCount){
        ArrayList<Short> shards = new ArrayList<Short>();
        for(int i = 0;i < utxoes.size();i++){
            String utxo = utxoes.get(i);
            byte[] byteBuffer = null;
            try {
                MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                messageDigest.update(utxo.getBytes());
                byteBuffer = messageDigest.digest();
            }catch(NoSuchAlgorithmException e){
                e.printStackTrace();
            }
            int j = 0;
            StringBuilder sb = new StringBuilder();
            while(bitCount > 0){
                if((bitCount/8) > 0) {
                    sb.append(getBitString(byteBuffer[j], 8));
                    j++;
                }else{
                    sb.append(getBitString(byteBuffer[j],bitCount));
                }
                bitCount -= 8;
            }
            short order = Short.valueOf(sb.toString(),2);
            shards.add(order);
        }
        return shards;
    }
    public  String  getBitString(byte b,int count) {
        byte operator = 1;
        StringBuilder sb =new StringBuilder();
        for(int i=7;i>=8-count;i--) {
            byte result = (byte) ((b>>>i) & operator);
            sb.append(result == 0?"0":"1");

        }
        return  sb.toString();
    }
}

