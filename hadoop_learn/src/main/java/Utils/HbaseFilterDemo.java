package Utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.io.Resources;

public class HbaseFilterDemo {
	private static Connection conn = null;
	
	public HbaseFilterDemo(){
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(Resources.getResource("hbase-site.xml"));
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			conn = null;
		}
	}
	/**Scan 全表扫描的方式
	 * @throws IOException 
	 * */
	public void orderRowDemo() throws IOException{
		TableName tablename = TableName.valueOf("testTable");
		Table table = conn.getTable(tablename);
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("testRowKey1"));
		scan.setStopRow(Bytes.toBytes("testRowKey2"));
		ResultScanner rs = table.getScanner(scan);
		getResultScanner(rs);
	}
	/**行键过滤器实例
	 * @throws IOException 
	 * */
	public void rowFilterDemo() throws IOException{
		TableName tablename = TableName.valueOf("testTable");
		Table table = conn.getTable(tablename);
		BinaryComparator binCom = new BinaryComparator(Bytes.toBytes("testRowKey3"));
		Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,binCom);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		getResultScanner(rs);
	}
	/**列簇过滤器演示实例
	 * @throws IOException 
	 * */
	public void familyFilterDemo() throws IOException{
		TableName tablename = TableName.valueOf("testTable");
		Table table = conn.getTable(tablename);
		BinaryComparator binCom = new BinaryComparator(Bytes.toBytes("family1"));
		Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,binCom);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		getResultScanner(rs);
	}
	/**子列对比过滤器
	 * @throws IOException 
	 * */
	public void columnFilterDemo() throws IOException{
		TableName tablename = TableName.valueOf("testTable");
		Table table = conn.getTable(tablename);
		RegexStringComparator binCom = new RegexStringComparator("column");
		Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,binCom);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		getResultScanner(rs);
	}
	/**列值过滤器
	 * @throws IOException 
	 * */
	public void valueFilterDemo() throws IOException{
		TableName tablename = TableName.valueOf("testTable");
		Table table = conn.getTable(tablename);
		BinaryComparator binCom = new BinaryComparator(Bytes.toBytes("value10"));
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,binCom);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		getResultScanner(rs);
	}
	/**行键前缀过滤器演示实例
	 * @throws IOException 
	 * */
	public void prefixFilterDemo() throws IOException{
		TableName tablename = TableName.valueOf("testTable");
		Table table = conn.getTable(tablename);
		PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("testRowkey1"));
		Scan scan = new Scan();
		scan.setFilter(prefixFilter);
		ResultScanner rs = table.getScanner(scan);
		getResultScanner(rs);
	}
	/**遍历结果集
	 * 
	 * */
	private void getResultScanner(ResultScanner rs){
		for(Result result : rs){
			List<Cell> list = result.listCells();
			if(list != null && list.size()>0){
				System.out.println("Cell 总尺寸:"+list.size());
				for(Cell cell:list){
					byte[] bytes = CellUtil.cloneRow(cell);
					if(bytes != null){
						System.out.println("行键:"+Bytes.toString(bytes));
					}
					bytes = CellUtil.cloneFamily(cell);
					if(bytes != null){
						System.out.println("列簇:"+Bytes.toString(bytes));
					}
				}
			}
		}
	}
}
