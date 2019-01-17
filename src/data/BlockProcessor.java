package data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import DBUtil.DBOperation;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

//每500块为一组，读入地址并分配id(使用Mysql)
public class BlockProcessor {
    private int afterSharding_shards_count = 0;
    private int rand_shards_count = 0;
    DBOperation dbOper = null;
    public void readBlock(String dir,int shardSeqNo) throws Exception{
        File blockDir = new File(dir);
        File blockData = null;
        dbOper = new DBOperation();
        dbOper.openConnection("jdbc:mysql://localhost:3306/btc?user=root&password=mzl123");
        StringBuilder sb = new StringBuilder();
        JSONObject block_json_obj = null;
        for(File file:blockDir.listFiles()){
            blockData = file;
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
            JSONArray jsonArr = block_json_obj.getJSONArray("blocks");
            int blocks_size = jsonArr.size();
            String addr = null;
            int blkHeight = -1;
            int randShard = 0;
            int notRandShard = 0;
            for(int i = 0;i<blocks_size;i++){
                JSONObject block = jsonArr.getJSONObject(i);
                blkHeight = block.getIntValue("height");
                JSONArray txs= block.getJSONArray("tx");
                int tx_size = txs.size();
                System.out.println(tx_size);//打印交易数
                HashSet<String> blkAddrs = new HashSet<String>();
                ArrayList<String[]> edges = new ArrayList<String[]>();
                for(int j = 0;j < tx_size;j++){
                    ArrayList<String> tx_utxoes  = new ArrayList<String>();
                    ArrayList<String> checkTxAddrs = new ArrayList<String>();
                    HashSet<String> txAddrs = new HashSet<String>();
                    JSONObject tx = txs.getJSONObject(j);
                    JSONArray inputs = tx.getJSONArray("inputs");
                    int input_size = inputs.size();
                    for(int k = 0;k < input_size;k++){
                        JSONObject input = inputs.getJSONObject(k);
                        JSONObject prev_out = input.getJSONObject("prev_out");
                        if(prev_out != null){
                            addr = prev_out.getString("addr");
                            txAddrs.add(addr);
                            checkTxAddrs.add(addr);
                            tx_utxoes.add(prev_out.getString("tx_index")+addr+prev_out.getString("value")+prev_out.getString("script"));
                        }
                    }
                    JSONArray outs = tx.getJSONArray("out");
                    int outSize = outs.size();
                    for(int k = 0;k<outSize;k++){
                        JSONObject out = outs.getJSONObject(k);
                        addr = out.getString("addr");
                        if(addr != null){
                            txAddrs.add(addr);
                            checkTxAddrs.add(addr);
                            tx_utxoes.add(out.getString("tx_index")+addr+out.getString("value")+out.getString("script"));
                        }
                    }
                    for(String addr1:txAddrs){
                        for(String addr2:txAddrs){
                            String[] edge = new String[]{addr1,addr2};
                            edges.add(edge);
                        }
                    }
                    blkAddrs.addAll(txAddrs);
                    if(shardSeqNo != 0){
                        afterSharding_shards_count += getAfterShardingShardsCount(checkTxAddrs);
                        rand_shards_count += getRandShardCount(tx_utxoes,10);
                    }
                }
                dbOper.insertAddrBatch(blkAddrs);
                dbOper.insertEdgeBatch(edges);
                if(shardSeqNo == 0) {
                    if (dbOper.getActiveAddrsCount() / (dbOper.getAddrCount() * 1.0) >= thresHold) {
                        sharding.init();
                        System.out.println(file.getName());
                    }
                }
                if(shardSeqNo != 0){
                    PrintWriter writer = new PrintWriter(new PrintStream(new FileOutputStream("")));
                    System.out.println(afterSharding_shards_count/(rand_shards_count*1.0));
                }
            }
        }
        dbOper.closeConnection();
    }
    public int  getAfterShardingShardsCount(ArrayList<String> addrs) throws Exception{
        HashSet<Byte> shards_count = new HashSet<Byte>();
        for(String addr:addrs){
            byte shard_id = dbOper.getAddrShard(addr);
            shards_count.add(shard_id);
        }
        return shards_count.size();
    }
    //细粒度的统计：是以块为单位的
    //但这里我们以单个交易额UTXO为单位进行统计可以减少内存的UTXO存储，
    public int getRandShardCount(ArrayList<String> utxoes,int bitCount){
        HashSet<Short> shardSet = new HashSet<Short>();
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
            shardSet.add(order);
        }
        return shardSet.size();
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

