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
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url,"root","admin123#");
            stat = conn.createStatement();
        }catch(ClassNotFoundException e){
            System.out.println("jdbc mysql driver is not found");
            closeConnection();
        }catch(SQLException e){
        	System.out.println("SQL Exception");
            closeConnection();
        }finally {
            
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
    //done
    public void allocateAddrsIdsBatch(ArrayList<String> addrs){
    	//Sql语句含义：首先设置自增量的起始值连续。然后ignore防止插入相同时报错
    	try {
    		for(int i = 0;i<addrs.size();i++) {
    			String flushAutoIncSql = "alter table id_addr_tbl AUTO_INCREMENT=1";
    			String insertSql = "insert ignore into id_addr_tbl(addr,cluster_id_new) values('" + addrs.get(i) + "',-1)";
    			stat.addBatch(flushAutoIncSql);
    			stat.addBatch(insertSql);
    		}      
				stat.executeBatch();
			} catch (SQLException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
    //查询操作:
    //done
    public int getAddrId(String addr) {
    	int id = -1;
    	try {
			ResultSet rs = stat.executeQuery("select id from id_addr_tbl where addr= '"+ addr + "'");
			if(rs.next()) {
				id = rs.getInt("id");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return id;
    }
    //批量查询地址id
    //done
    public HashMap<String,Integer> getAddrIdMap(ArrayList<String> addrs){
    	HashMap<String,Integer> addrIdMap = new HashMap<String,Integer>();
    	try {
			CallableStatement cs = conn.prepareCall("CALL ADDR_ID_PAIRS (?)");
			String sql_para = "(";
			for(int i = 0;i<addrs.size();i++) {
				if(i<addrs.size()-1)
					sql_para += ("\'" +addrs.get(i)+ "\'"+",");
				else
					sql_para += ("\'" + addrs.get(i) + "\'" + ")");
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
    //查询cluster_id  根据 地址id
    //done
    public int getClusterIdById(int id) {
    	int clusterId  = -1;    	
		try {
			String sql = "select cluster_id_new from id_addr_tbl where id="+id;
		    ResultSet rs = stat.executeQuery(sql);
		    if(rs.next())
		    	clusterId = rs.getShort("cluster_id_new");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return clusterId;
    }
    //批量获取地址簇id
    //done
    public HashMap<Integer,Integer> getIdClusterIdMap(ArrayList<Integer> ids) {
    	HashMap<Integer,Integer> idClusterIdMap = new HashMap<Integer,Integer>();
    	try {
			CallableStatement cs = conn.prepareCall("CALL ID_CLUSTERID_PAIRS(?)");
			String sql_para = "(";
			for(int i = 0;i<ids.size();i++) {
				if(i<ids.size()-1)
					sql_para += (ids.get(i) + ",");
				else
					sql_para += (ids.get(i) + ")");
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
    //获得地址簇id By addr
    //done
    public int getClusterIdByAddr(String addr) {
    	int clusterId = -1;
    	String sql = "select cluster_id_new from id_addr_tbl where addr= \""+ addr + "\"";
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
    //获得地址-地址簇的字典对
    //done
    public HashMap<String,Integer> getAddrClusterIdMap(ArrayList<String> addrs){
    	HashMap<String,Integer> addrClusterIdMap = new HashMap<String,Integer>();
    	try {
			CallableStatement cs = conn.prepareCall("CALL ADDR_CLUSTERID_PAIRS(?)");
			String sql_para = "(";
			for(int i = 0;i<addrs.size();i++) {
				if(i<addrs.size()-1)
					sql_para += ("\'" + addrs.get(i) + "\',");
				else
					sql_para += ("\'" + addrs.get(i) + "\')");
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
    //done
    public void clusterIdPreProcess() {
    	String sql = "update id_addr_tbl set cluster_id_old = cluster_id_new,cluster_id_new = -1";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //通过地址id更新clusterId
    //done
    public void updateClusterIdById(int id,int cluster_id_new) {
    	String sql = "update id_addr_tbl set cluster_id_new =" + cluster_id_new +" where id=" + id;
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //更新clusterId By 地址
    //done
    public void updateClusterIdByAddr(String addr,int cluster_id_new) {
    	String sql = "update id_addr_tbl set cluster_id_new =" + cluster_id_new +" where addr=" + "\'" + addr + "\'";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //批量更新地址的地址簇号
    //done
    public void updateClusterIdBatch(HashMap<Integer,Integer> idClusterMap) {
    	try {
    		for(int id:idClusterMap.keySet()) {
    			String sql = "update id_addr_tbl set cluster_id_new="+idClusterMap.get(id) + " where id=" + id;
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
    //使用的映射是映射到n^0.75，这个参数可以调整。
    //done
    public void insertEdgesBatch(List<String> addrIds) {
    	try {
    		for(int i=0;i<addrIds.size();i++) {
    			String sql = "insert into edge_weight_tbl(addr1,addr2,acc_weight,epoch_weight,is_updated) values("
    					+addrIds.get(i).split(",")[0]+","+addrIds.get(i).split(",")[1]+",0,1,1) "
    							+ "on duplicate key update epoch_weight = epoch_weight + 1,is_updated = 1";
				stat.addBatch(sql);	
    		}
    		stat.executeBatch();
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
    //更新acc_weight值
    //done
    public void flushEdgesAccWeight() {
    	String sql = "update edge_weight_tbl set acc_weight=if(acc_weight<0,pow(epoch_weight,0.75),"
    			+ "pow(pow(acc_weight,0.75)+epoch_weight,0.75))  where is_updated = 1";
    	try {
			stat.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //每个新的地址聚类之前重新将epoch_weight设置为0
    //done
    public void edgesEpochWeightPreProcess() {
    	String sql = "update edge_weight_tbl set epoch_weight = 0";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //delete  在每次进行图的重分割之前进行处理
    //done
    public void deleteEdges(int threshold) {
    	String sql = "delete from edge_weight_tbl where acc_weight<" + threshold ;
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //update  对于不活跃的边权值递减
    //done
    public void decreaseEdgesWeight(float decay_rate) {
    	try {
    			String sql = "update edge_weight_tbl set acc_weight = acc_weight * " + decay_rate +"where is_updated=0";
				stat.executeUpdate(sql);
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
    //在一轮新的地址后将更新字段设置为0
    //done
    public void flushUpdated() {
    	String sql = "update edge_weight_tbl set is_updated = 0";
    	try {
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //retrieve  *****条件,加参数  构图
    //
    public HashMap<Integer,ArrayList<Float>> getEpochEdges(int limit) {
    	HashMap<Integer,ArrayList<Float>> epochEdges = new HashMap<Integer,ArrayList<Float>>();
    	String sql = "select addr1,addr2,acc_weight from edge_weight_tbl where is_updated = 1 and acc_weight>" + limit;
    	try {
			ResultSet rs = stat.executeQuery(sql);
			while(rs.next()) {
				int id1 = rs.getInt("addr1");
				int id2 = rs.getInt("addr2");
				float weight = rs.getFloat("acc_weight");
				if(epochEdges.containsKey(id1)) {
					epochEdges.get(id1).add((float)id2);
					epochEdges.get(id1).add(weight);
				}
				else {
					ArrayList<Float> list = new ArrayList<Float>();
					list.add((float)id2);
					list.add(weight);
					epochEdges.put(id1, list);
				}
				if(epochEdges.containsKey(id2)) {
					epochEdges.get(id2).add((float)id1);
					epochEdges.get(id2).add(weight);
				}
				else {
					ArrayList<Float> list = new ArrayList<Float>();
					list.add((float)id1);
					list.add(weight);
					epochEdges.put(id2, list);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return epochEdges;
    }
    //将未参与地址图划分的地址重新进行地址簇划分
    //done
    public void updateUnClusteredId(int clusterCount) {
    	int denseClusterId = -1;
    	for(int i = 1;i<=clusterCount;i++) {
    		String sql = "select cluster_id_new from id_addr_tbl where cluster_id_old=" + i
    				+" and cluster_id_new!=-1 group by cluster_id_new order by count(*) desc limit 1";
    		try {
				ResultSet rs = stat.executeQuery(sql);			
				if(rs.next()) {
					denseClusterId = rs.getInt("cluster_id_new");
				}
				String flushAddrSql = "update id_addr_tbl set cluster_id_new=" + denseClusterId + " where cluster_id_old=" + i
						+ " and cluster_id_new=-1";
				stat.executeUpdate(flushAddrSql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    	}
    }
}