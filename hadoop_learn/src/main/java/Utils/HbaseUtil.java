package Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StringUtils;

import com.google.common.io.Resources;

import jodd.util.StringUtil;

/**Hbase工具类
 * Created by liwenxiang on 2018/5/4
 * */

public class HbaseUtil {
	
	private static Connection conn = null;
	
	private static HbaseUtil hbaseUtil;
	
	/**构造HbaseUtil的单例模式，懒汉式的
	 * */
	private HbaseUtil(){
		
	}
	
	/**获取Hbase工具类实例
	 * */
	public static HbaseUtil getSingle(){
		if(hbaseUtil == null){
			hbaseUtil = new HbaseUtil();
		}
		if(conn == null){
			Configuration config = HBaseConfiguration.create();
			config.addResource(Resources.getResource("hbase-site.xml"));
			try {
				conn = ConnectionFactory.createConnection(config);
			} catch (IOException e) {
				
				e.printStackTrace();
				conn = null;
			}
		}
		return hbaseUtil;
	}
	/**获取已经打开的连接
	 * */
	public Connection getConn(){
		return conn;
	}
	/**判断表是否存在并且是可用状态
	 * @param tableName 表名
	 * @return 存在返回true,否则返回false
	 * @throws IOException 
	 * */
	public boolean existsTable(String tableName) throws IOException{
		if(StringUtil.isEmpty(tableName)){
			return false;
		}
		Admin admin = conn.getAdmin();//操作hbase先获取管理对象
		TableName tablename = TableName.valueOf(tableName);
		if(admin.tableExists(tablename) && admin.isTableEnabled(tablename))
		{
			return true;
		}else{
			return false;
		}
	}
	/**创建一张表
	 * @param tableName	表名
	 * @param columnFamilys 列簇集合
	 * @return 
	 * @throws IOException 
	 * */
	public boolean createTable(String tableName,String columnFamilys[]) throws IOException{
		if(StringUtils.isEmpty(tableName)||columnFamilys.length<1){
			return false;
		}
		Admin admin = conn.getAdmin();
		TableName tablename = TableName.valueOf(tableName);
		HTableDescriptor htable = new HTableDescriptor(tablename);
		for(String family : columnFamilys){
			HColumnDescriptor column = new HColumnDescriptor(family);
			htable.addFamily(column);
		}
		admin.createTable(htable);
		return true;
	}
	/**删除表
	 * @param tableName 表名
	 * @return 删除成功返回true,否则返回false
	 * @throws IOException 
	 * */
	public boolean deleteTable(String tableName) throws IOException{
		if(StringUtils.isEmpty(tableName)){
			return false;
		}
		Admin admin = conn.getAdmin();
		TableName tablename = TableName.valueOf(tableName);
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
		return true;
	}
	/**添加一行数据(添加数据时一定记得将数据转成二进制字节）
	 * @param tableName 表名
	 * @param row	行号
	 * @param columnFamily	列簇
	 * @param column	子列
	 * @param value	 具体的值
	 * @return 成功返回true,否则返回false
	 * @throws IOException 
	 * */
	public boolean insertRow(String tableName,String rowkey,String columnFamily,
			String column,String value ) throws IOException{
		if (StringUtil.isEmpty(tableName) || StringUtil.isEmpty(columnFamily) || StringUtil.isEmpty(rowkey)
                || StringUtil.isEmpty(value)) {
            return false;

        }
		if(!existsTable(tableName)){
			return false;
		}
		TableName tablename = TableName.valueOf(tableName);
		Table table = conn.getTable(tablename);
		Put put = new Put(Bytes.toBytes(rowkey));//此时不需要连接，直接操作表，admin是管理表的操作
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		table.close();
		return true;
	}
	/**
     * 批量添加数据
     *
     * @param tableName     表名
     * @param rows          行号集合
     * @param columnFamilys 列簇集合
     * @param columns       子列集合 list不能为空，list中的值可以为空
     * @param values        列值集合
     * @return 添加成功返回true，否则返回false
     * @throws IOException
     */
    public boolean insertRow(String tableName, List<String> rows,
                             List<String> columnFamilys, List<String> columns, List<String> values) throws IOException {
        if (StringUtil.isEmpty(tableName) || rows == null || columnFamilys == null || columns == null
                || values == null) {
            return false;
        }
        if (
                columns.size() != values.size()) {
            return false;
        }
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = conn.getTable(tableName1);
        List<Put> puts = new ArrayList<>(rows.size());
        for (int cnt = 0; cnt < rows.size(); cnt++) {
            Put put = new Put(Bytes.toBytes(rows.get(cnt)));
            if (StringUtil.isNotEmpty(columns.get(cnt))) {
                put.addColumn(Bytes.toBytes(columnFamilys.get(cnt)),
                        Bytes.toBytes(columns.get(cnt)), Bytes.toBytes(values.get(cnt)));
            } else {
                put.addColumn(Bytes.toBytes(columnFamilys.get(cnt)),
                        null, Bytes.toBytes(values.get(cnt)));
            }
            puts.add(put);

        }
        table.put(puts);
        return false;
    }
    /**批量添加数据
     * @param tableName	表名
     * @param rows 行号集合
     * @param columnFamily 统一列簇
     * @param column 统一子列
     * @param values 列值集合
     * @return 添加成功返回true,否则返回false
     * @throws IOException 
     * */
    public boolean insertRow(String tableName,List<String> rows,String columnFamily,
    		String column,List<String> values) throws IOException{
    	  if (StringUtil.isEmpty(tableName) || StringUtil.isEmpty(columnFamily)
                  || StringUtil.isEmpty(column) || rows == null || values == null) {
              return false;
          }
    	  if(rows.size() != values.size()){
    		  return false;
    	  }
    	  List<Put> putlist = new ArrayList<Put>();
    	  for(int cnt = 0;cnt<rows.size();cnt++){
    		  Put put = new Put(Bytes.toBytes(rows.get(cnt)));
    		  byte[] familyBytes = Bytes.toBytes(columnFamily);
    		  byte[] columnBytes = null;
    		  if(StringUtil.isNotEmpty(column)){
    			  columnBytes = Bytes.toBytes(column);
    		  }
    		  put.addColumn(familyBytes, columnBytes,Bytes.toBytes(values.get(cnt)));
    		  putlist.add(put);
    	  }
    	  Table table = conn.getTable(TableName.valueOf(tableName));
    	  table.put(putlist);
    	  table.close();
    	  return true;
    }
    /**获取一行数据
     * @param parameter 传入参数的顺序：表名，行键，列簇（可为空），子列（可为空）
     * @return 查询到的数据
     * @throws IOException 
     * */
    public List<Cell> getData(String... parameter) throws IOException{
    	if(parameter == null || parameter.length<2){
    		return null;
    	}
    	if(StringUtil.isEmpty(parameter[0]) || StringUtil.isEmpty(parameter[1])){
    		return null;
    	}
    	TableName tablename = TableName.valueOf(parameter[0]);
    	Table table =conn.getTable(tablename);
    	Get get = new Get(Bytes.toBytes(parameter[1]));
    	if(parameter.length>2 && StringUtil.isNotEmpty(parameter[2])){
    		get.addFamily(Bytes.toBytes(parameter[2]));
    	}
    	if(parameter.length>3 && StringUtil.isNotEmpty(parameter[3])){
    		get.addColumn(Bytes.toBytes(parameter[2]), Bytes.toBytes(parameter[3]));
    	}
    	Result result = table.get(get);
    	List<Cell> list = result.listCells();
    	table.close();
    	return list;
    }
    /**根据row,批量获取多行记录
     * @param tableName : 表名
     * @param rows 行键
     * @return 查询到的数据
     * @throws IOException 
     * */
    public List<List<Cell>> getData(String tableName,List<String> rows) throws IOException{
    	 if (StringUtil.isEmpty(tableName) || rows == null || rows.size() < 1) {
             return null;
         }
    	 Table table = conn.getTable(TableName.valueOf(tableName));
    	 List<Get> getlist = new ArrayList<>(rows.size());
    	 for(String row : rows){
    		 if(StringUtil.isEmpty(row)){
    			 continue;
    		 }
    		 Get get = new Get(Bytes.toBytes(row));
    		 getlist.add(get);
    	 }
    	 Result[] results = table.get(getlist);
    	 List<List<Cell>> returnValue = null;
    	 if(results != null && results.length>0){
    		 returnValue = new ArrayList<>(results.length);
    		 for(Result result : results){
    			 returnValue.add(result.listCells());
    		 }
    	 }
    	 return returnValue;
    }
    /**删除一条数据
     * @param tableName 表名
     * @param row 行键
     * @return 删除成功返回true,否则返回false
     * @throws IOException 
     * */
    public boolean delRow(String tableName,String row) throws IOException{
    	  if (StringUtil.isEmpty(tableName) || StringUtil.isEmpty(row)) {
              return false;
          }
    	  TableName tablename = TableName.valueOf(tableName);
    	  Table table = conn.getTable(tablename);
    	  Delete delete = new Delete(Bytes.toBytes(row));
    	  table.delete(delete);
    	  table.close();
    	  return true;
    }
    /**批量删除数据
     * @param tableName 表名
     * @param rows 行键集合
     * @return 删除成功返回true,否则返回false
     * @throws IOException 
     * */
    public boolean delRow(String tableName,List<String> rows) throws IOException{
    	  if (StringUtil.isEmpty(tableName) || rows == null || rows.size() < 1) {
              return false;
          }
    	  Table table = conn.getTable(TableName.valueOf(tableName));
    	  List<Delete> deleteList = new ArrayList<>(rows.size());
    	  for(String row : rows){
    		  if(StringUtil.isEmpty(row)){
    			  continue;
    		  }
    		  Delete delete = new Delete(Bytes.toBytes(row));
    		  deleteList.add(delete);
    	  }
    	  table.delete(deleteList);
    	  table.close();
    	  return true;
    }
    /**关闭连接
     * */
    public void closeConn(){
    	if(conn != null){
    		try {
				conn.close();
			} catch (IOException e) {
			
				e.printStackTrace();
			}
    	}
    }
}
