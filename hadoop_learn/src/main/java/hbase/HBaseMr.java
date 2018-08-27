package hbase;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/5/3.
 */
public class HBaseMr {
    /**创建hbase配置
     * */
    static Configuration config = null;
    static{
        config = HBaseConfiguration.create();
        config.addResource(Resources.getResource("hbase-site.xml"));
    }
    /**表信息
     * */
    public static final String tableName = "word";//表名
    public static final String colf = "content";//列族
    public static final String col = "info";//列
    public static final String tableName2 = "stat";//表名
    /**初始化表结构，及其数据
     * */
    public static void initTB(){
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(config);//创建表管理
            if(admin.tableExists(tableName)||admin.tableExists(tableName2)){
                System.out.println("table is already exists");
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                admin.disableTable(tableName2);
                admin.deleteTable(tableName2);
            }
            /**创建表
             * */
            HTableDescriptor desc = new HTableDescriptor(tableName);
            HColumnDescriptor family = new HColumnDescriptor(colf);
            desc.addFamily(family);
            admin.createTable(desc);
            HTableDescriptor desc2 = new HTableDescriptor(tableName2);
            HColumnDescriptor family2 = new HColumnDescriptor(colf);
            desc2.addFamily(family2);
            admin.createTable(desc2);
            /**插入数据(现在可能主要用获取链接的方式啦）
             * */
            table = new HTable(config,tableName);
            table.setAutoFlush(false);
            List<Put> lp = new ArrayList<Put>();
            Put p1 = new Put(Bytes.toBytes("1"));
            p1.addColumn(colf.getBytes(),col.getBytes(),("The Apache Hadoop software library is a framework").getBytes());
            lp.add(p1);
            Put p2 = new Put(Bytes.toBytes("2"));
            p2.addColumn(colf.getBytes(),col.getBytes(),("The common utilities that support the other Hadoop modules").getBytes());
            lp.add(p2);
            Put p3 = new Put(Bytes.toBytes("3"));
            p3.addColumn(colf.getBytes(), col.getBytes(),("Hadoop by reading the documentation").getBytes());
            lp.add(p3);
            Put p4 = new Put(Bytes.toBytes("4"));
            p4.addColumn(colf.getBytes(), col.getBytes(),("Hadoop from the release page").getBytes());
            lp.add(p4);
            Put p5 = new Put(Bytes.toBytes("5"));
            p5.addColumn(colf.getBytes(), col.getBytes(),("Hadoop on the mailing list").getBytes());
            lp.add(p5);
            table.put(lp);
            table.flushCommits();
            lp.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try{
                if(table!=null){
                        table.close();
                    }
                }catch(IOException e){
                e.printStackTrace();
            }
        }
    }
    /**MyMapper继承TableMapper
     * TableMapper<Text,IntWritable>
     *Text :输出的key类型
     * IntWritable : 输出的value类型
     * */
    public static class MyMapper extends TableMapper<Text,IntWritable>{
        private static IntWritable one = new IntWritable(1);
        private static Text word = new Text();
        @Override
        //输入的类型:key:rowKey;value:一行数据的结果集Result
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
           //获取一行数据中的colf : col
            String words = Bytes.toString(value.getValue(Bytes.toBytes(colf),Bytes.toBytes(col)));
            //按空格分割
            String itr[] = words.split(" ");
            //循环输出word和1
            for(int i = 0;i<itr.length;i++){
                word.set(itr[i]);
                context.write(word,one);
            }
        }
    }
    /**MyReducer继承TableReducer
     * TableReducer<Text,IntWritable>
     * Text:输入的key类型
     * IntWritable：输入的value类型
     * ImmutableBytesWritable：输出类型，表示rowkey的类型
     * */
    public static class MyReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //对mapper的数据求和
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            //创建put,设置rowkey为单词
            Put put = new Put(Bytes.toBytes(key.toString()));
            //封装数据
            put.add(Bytes.toBytes(colf), Bytes.toBytes(col),Bytes.toBytes(String.valueOf(sum)));
            //写到hbase,需要指定rowkey、put
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        config.set("df.default.name","hdfs://master1.hadoop:9000/");//设置hdfs默认路径
        //初始化表
        initTB();
        //创建job
        Job job = Job.getInstance(config);
        job.setJarByClass(HBaseMr.class);

        //创建scan
        Scan scan = new Scan();

        //可以指定查询某一列
        scan.addColumn(Bytes.toBytes(colf), Bytes.toBytes(col));
        //创建查询hbase的mapper，设置表名、scan、mapper类、mapper的输出key、mapper的输出value
        TableMapReduceUtil.initTableMapperJob(tableName, scan, MyMapper.class,Text.class, IntWritable.class, job);
        //创建写入hbase的reducer，指定表名、reducer类、job
        TableMapReduceUtil.initTableReducerJob(tableName2, MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
