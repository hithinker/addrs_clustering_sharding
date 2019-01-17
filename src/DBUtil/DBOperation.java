package DBUtil;

import java.sql.*;
import java.util.*;

import addrs_clustering_sharding.Graph;
import addrs_clustering_sharding.NodeEdgeInfo;
//对于批处理操作的理解:是基于JDBC操作来说的批量操作。JDBC可以批量插入、删除、更新，但是不能批量查询
//批量查询的含义是多条sql语句，主要的原因是无法处理多次查询的结果
//数据库操作:1.addr_id_tbl表操作:a.分配地址id,批量插入操作 b.更新 (1)批量使用cluster_id_new来覆盖cluster_id_old,并指定
//shard_id_new全为-1(初始时指定shard_id_new为-1),批量更新cluster_id_new列c.查询(1)查询addr的id,单个/批量 (2)单个/批量查询地址cluster_id
//注意:JDBC是不支持批量查询操作的，使用存储过程，中间注意的问题是使用字符串连接的方式来执行
//数据库操作:2.edge_weight_tbl表操作:a插入 (1)插入一条边，注意插入时的语句on duplicated key 批量插入  b删除  批量删除，将边权值在一定
//阈值之下的删除 c更新 (1)批量更新is_updated 字段为0(初始时也设置为0) d查询 批量查询is_updated为1的边及权值来构图

public class DBOperation {
    private Connection conn;
    private Statement stat;
    public void openConnection(String url){
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            conn  = DriverManager.getConnection(url);
            stat = conn.createStatement();
        }catch(ClassNotFoundException e){
            System.out.println("jdbc mysql driver is not found");
        }catch(InstantiationException e){
            System.out.println("jdbc mysql driver instantiating fail");
        }catch (IllegalAccessException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
        }finally {
            closeConnection();
        }
    }
    public void closeConnection(){
        if(stat != null){
            try {
                stat.close();
                conn.close();
        }catch (SQLException e) {
                System.out.println("DB connection closing error!");
            }
            stat = null;
            conn = null;
        }
    }
    //表id_addr_tbl表的操作
    //插入操作
    
    
    //由于比特币地址为200bits,将上百万地址串读入内存将会占据大量的内存空间，因此需要将地址映射为int型的字段，
    //int 型为32bit，是十亿量级，满足我们的实验需求和实际上使用的比特币地址总数，因此是可行的方案
    //设计存储比特币地址和int型id的映射表，id唯一主键，addr为唯一型键,有addr->id.同时设置cluster_id_new字段为-1。
    public void allocateAddrsIdsBatch(String[] addrs){
    	//Sql语句含义：首先设置自增量的起始值连续。然后ignore防止插入相同时报错
    	try {
    		for(int i = 0;i<addrs.length;i++) {
    			String sql = "alter table id_addr_tbl AUTO_INCREMENT=1;insert ignore into id_addr_tbl(addr,cluster_id_new) "
            		+ "values("+ addrs[i] + ",-1)";
    			stat.addBatch(sql);
    		}
       
				stat.executeBatch();
			} catch (SQLException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
    //查询操作:
    public int getAddrId(String addr) {
    	int id = -1;
    	try {
			ResultSet rs = stat.executeQuery("select id from id_addr_tbl where addr="+addr);
			if(!rs.next()) id = rs.getInt("id");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return id;
    }
    public HashMap<String,Integer> getAddrIdMap(String[] addrs){
    	HashMap<String,Integer> addrIdMap = new HashMap<String,Integer>();
    	try {
			CallableStatement cs = conn.prepareCall("CALL ADDR_ID_PAIRS (?)");
			String sql_para = "(";
			for(int i = 0;i<addrs.length;i++) {
				if(i<addrs.length-1)
					sql_para += ("\"" +addrs[i]+ "\""+",");
				else
					sql_para += ("\"" + addrs[i] + "\"" + ")");
			}
			cs.setString(1,sql_para);
			cs.execute();
			ResultSet rs = cs.getResultSet();
			while(rs.next()) 
				addrIdMap.put(rs.getString("addr"),rs.getInt("id"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return addrIdMap;
    	}
    public int getClusterIdById(int id) {
    	int clusterId  = -1;    	
		try {
			String sql = "select cluster_id_new from addr_id_tbl where id="+"id";
		    ResultSet rs = stat.executeQuery(sql);
		    if(rs.next())
		    	rs.getString("cluster_id_new");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return clusterId;
    }
    public Map<Integer,Integer> getIdClusterIdMap(int[] ids) {
    	HashMap<Integer,Integer> idClusterIdMap = new HashMap<Integer,Integer>();
    	try {
			CallableStatement cs = conn.prepareCall("CALL ID_CLUSTERID_PAIRS(?)");
			String sql_para = "(";
			for(int i = 0;i<ids.length;i++) {
				if(i<ids.length-1)
					sql_para += (ids[i] + ",");
				else
					sql_para += (ids[i] + ")");
			}
			cs.setString(1,sql_para);
			cs.execute();
			ResultSet rs = cs.getResultSet();
			while(rs.next()) 
				idClusterIdMap.put(rs.getInt("id"),rs.getInt("cluster_id_new"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return idClusterIdMap;
    	}
    public int getClusterIdByAddr(String addr) {
    	int clusterId = -1;
    	String sql = "select cluster_id_new from addr_id_tbl where addr= \""+ addr + "\"";
    	ResultSet rs;
		try {
			rs = stat.executeQuery(sql);
			if(rs.next()) clusterId = rs.getInt("cluster_id_new");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return clusterId;
    	}
    public HashMap<String,Integer> getAddrClusterIdMap(String[] addrs){
    	HashMap<String,Integer> addrClusterIdMap = new HashMap<String,Integer>();
    	try {
			CallableStatement cs = conn.prepareCall("CALL ADDR_CLUSTERID_PAIRS(?)");
			String sql_para = "(";
			for(int i = 0;i<addrs.length;i++) {
				if(i<addrs.length-1)
					sql_para += ("\"" + addrs[i] + "\",");
				else
					sql_para += ("\"" + addrs[i] + "\")");
			}
			cs.setString(1,sql_para);
			cs.execute();
			ResultSet rs = cs.getResultSet();
			while(rs.next()) 
				addrClusterIdMap.put(rs.getString("addr"),rs.getInt("cluster_id_new"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return addrClusterIdMap;
    	}
    //更新操作
    public void clusterIdPreProcess() {
    	String sql = "update id_addr_tbl set cluster_id_old = cluster_id_new,cluster_id_new = -1";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public void updateClusterIdById(int id,int cluster_id_new) {
    	String sql = "update id_addr_tbl set cluster_id_new =" + cluster_id_new +" where id=" + id;
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public void updateClusterIdByAddr(String addr,int cluster_id_new) {
    	String sql = "update id_addr_tbl set cluster_id_new =" + cluster_id_new +" where addr=" + "\"" + addr + "\"";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public void updateClusterIdBatch(HashMap<Integer,Integer> idClusterMap) {
    	try {
    		for(int id:idClusterMap.keySet()) {
    			String sql = "update id_addr_tbl set cluster_id_new="+idClusterMap.get(id) + "where id=" + id;
    			stat.addBatch(sql);
    		}
    
    		stat.executeBatch();
			} catch (SQLException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
    
   //表edge_weight表的操作
   //create  会遇到on duplicated key时的操作(加操作包含在了这一步里的重复边里)
    public void insertEdgesBatch(List<String> addrIds) {
    	try {
    		for(int i=0;i<addrIds.size();i++) {
    			String sql = "insert into table edge_weight_tbl(addr1,addr2,weight,is_updated) values("
    					+addrIds.get(i).split(",")[0]+","+addrIds.get(i).split(",")[1]+",1,1) "
    							+ "on duplicated key update weight = ";     		
				stat.addBatch(sql);	
    		}
    		stat.executeBatch();
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	} 
    //delete  在每次进行图的重分割之前进行处理
    public void deleteEdges() {
    	String sql = "delete * form edge_weight_tbl where weight < 0";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //update
    public void decreaseEdgesWeight() {
    	
    }
    public void flushUpdated() {
    	String sql = "update edge_weight_tbl set is_updated = 0";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //retrieve
    public HashMap<String,Integer> getEpochEdges() {
    	HashMap<String,Integer> epochEdges = new HashMap<String,Integer>();
    	String sql = "select addr1,addr2,weight from edge_weight_tbl where is_updated = 1";
    	try {
			ResultSet rs = stat.executeQuery(sql);
			while(rs.next()) {
				epochEdges.put(""+rs.getInt("addr1")+","+rs.getInt("addr2"), rs.getInt("weight"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return epochEdges;
    }
}