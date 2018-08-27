package cn.itcast.hive.mr.pre;

import cn.itcast.hive.mr.pre.cn.itcast.hive.mrbean.WebLogBean;
import cn.itcast.hive.mr.pre.cn.itcast.hive.mrbean.WebLogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by Administrator on 2018/3/2.
 */
public class WeblogPreValid {
    static class WeblogPreValidMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        //先假设一个url匹配库
        Set<String> pages = new HashSet<String>();
        Text k = new Text();
        NullWritable v = NullWritable.get();
        /*从外部加载网站url数据
        * */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            //过滤js/图片/css等静态资源
            WebLogParser.filtStaticResource( webLogBean,pages);

                k.set(webLogBean.toString());
                context.write(k, v);

        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);
        job.setJarByClass( WeblogPreValid.class);
        job.setMapperClass(WeblogPreValidMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("C:/data/input"));
        FileOutputFormat.setOutputPath(job,new Path("C:/data/output3"));
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }



}
