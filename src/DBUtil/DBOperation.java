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
    //��id_addr_tbl��Ĳ���
    //�������
    
    
    //���ڱ��رҵ�ַΪ200bits,���ϰ����ַ�������ڴ潫��ռ�ݴ������ڴ�ռ䣬�����Ҫ����ַӳ��Ϊint�͵��ֶΣ�
    //int ��Ϊ32bit����ʮ���������������ǵ�ʵ�������ʵ����ʹ�õı��رҵ�ַ����������ǿ��еķ���
    //��ƴ洢���رҵ�ַ��int��id��ӳ���idΨһ������addrΪΨһ�ͼ�,��addr->id.ͬʱ����cluster_id_new�ֶ�Ϊ-1��
    //done
    public void allocateAddrsIdsBatch(ArrayList<String> addrs){
    	//Sql��京�壺������������������ʼֵ������Ȼ��ignore��ֹ������ͬʱ����
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
    //��ѯ����:
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
    //������ѯ��ַid
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
    //��ѯcluster_id  ���� ��ַid
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
    //������ȡ��ַ��id
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
    //��õ�ַ��id By addr
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
    //��õ�ַ-��ַ�ص��ֵ��
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
    //���²���
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
    //ͨ����ַid����clusterId
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
    //����clusterId By ��ַ
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
    //�������µ�ַ�ĵ�ַ�غ�
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
    
    //��edge_weight��Ĳ���
    //create  ������on duplicated keyʱ�Ĳ���(�Ӳ�������������һ������ظ�����)
    //ʹ�õ�ӳ����ӳ�䵽n^0.75������������Ե�����
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
    //����acc_weightֵ
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
    //ÿ���µĵ�ַ����֮ǰ���½�epoch_weight����Ϊ0
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
    //delete  ��ÿ�ν���ͼ���طָ�֮ǰ���д���
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
    //update  ���ڲ���Ծ�ı�Ȩֵ�ݼ�
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
    //��һ���µĵ�ַ�󽫸����ֶ�����Ϊ0
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
    //retrieve  *****����,�Ӳ���  ��ͼ
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
    //��δ�����ַͼ���ֵĵ�ַ���½��е�ַ�ػ���
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