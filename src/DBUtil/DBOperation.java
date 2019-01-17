package DBUtil;

import java.sql.*;
import java.util.*;

import addrs_clustering_sharding.Graph;
import addrs_clustering_sharding.NodeEdgeInfo;
//������������������:�ǻ���JDBC������˵������������JDBC�����������롢ɾ�������£����ǲ���������ѯ
//������ѯ�ĺ����Ƕ���sql��䣬��Ҫ��ԭ�����޷������β�ѯ�Ľ��
//���ݿ����:1.addr_id_tbl�����:a.�����ַid,����������� b.���� (1)����ʹ��cluster_id_new������cluster_id_old,��ָ��
//shard_id_newȫΪ-1(��ʼʱָ��shard_id_newΪ-1),��������cluster_id_new��c.��ѯ(1)��ѯaddr��id,����/���� (2)����/������ѯ��ַcluster_id
//ע��:JDBC�ǲ�֧��������ѯ�����ģ�ʹ�ô洢���̣��м�ע���������ʹ���ַ������ӵķ�ʽ��ִ��
//���ݿ����:2.edge_weight_tbl�����:a���� (1)����һ���ߣ�ע�����ʱ�����on duplicated key ��������  bɾ��  ����ɾ��������Ȩֵ��һ��
//��ֵ֮�µ�ɾ�� c���� (1)��������is_updated �ֶ�Ϊ0(��ʼʱҲ����Ϊ0) d��ѯ ������ѯis_updatedΪ1�ı߼�Ȩֵ����ͼ

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
    //��id_addr_tbl��Ĳ���
    //�������
    
    
    //���ڱ��رҵ�ַΪ200bits,���ϰ����ַ�������ڴ潫��ռ�ݴ������ڴ�ռ䣬�����Ҫ����ַӳ��Ϊint�͵��ֶΣ�
    //int ��Ϊ32bit����ʮ���������������ǵ�ʵ�������ʵ����ʹ�õı��رҵ�ַ����������ǿ��еķ���
    //��ƴ洢���رҵ�ַ��int��id��ӳ���idΨһ������addrΪΨһ�ͼ�,��addr->id.ͬʱ����cluster_id_new�ֶ�Ϊ-1��
    public void allocateAddrsIdsBatch(String[] addrs){
    	//Sql��京�壺������������������ʼֵ������Ȼ��ignore��ֹ������ͬʱ����
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
    //��ѯ����:
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
    //���²���
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
    
   //��edge_weight��Ĳ���
   //create  ������on duplicated keyʱ�Ĳ���(�Ӳ�������������һ������ظ�����)
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
    //delete  ��ÿ�ν���ͼ���طָ�֮ǰ���д���
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