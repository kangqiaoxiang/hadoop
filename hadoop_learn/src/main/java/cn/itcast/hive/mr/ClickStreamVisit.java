package cn.itcast.hive.mr;

import cn.itcast.hive.mr.pre.cn.itcast.hive.mrbean.PageViewsBean;
import cn.itcast.hive.mr.pre.cn.itcast.hive.mrbean.VisitBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by Administrator on 2018/3/3.
 */
public class ClickStreamVisit {
    static class ClickStreamVisitMapper extends Mapper<LongWritable,Text,Text,PageViewsBean>{
        /*以session作为key,将数据发过去
        * */
        PageViewsBean pageViewsBean = new PageViewsBean();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\001");
            int step = Integer.parseInt(fields[5]);
            pageViewsBean.set(fields[0], fields[1], fields[2], fields[3],fields[4], step, fields[6], fields[7], fields[8], fields[9]);
            k.set(pageViewsBean.getSession());
            context.write(k,pageViewsBean);
        }
    }
    static class ClickStreamVisitReducer extends Reducer<Text,PageViewsBean,NullWritable,VisitBean> {
        @Override
        protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {
            //先将里面的pageviewBean全部取出来
            ArrayList<PageViewsBean> pvBeansList = new ArrayList<PageViewsBean>();
            PageViewsBean pageViewsBean = new PageViewsBean();
            for(PageViewsBean bean : values){
                try {
                    BeanUtils.copyProperties(pageViewsBean,bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                pvBeansList.add(pageViewsBean);
            }
            //按照step对beans里面的bean进行排序,
            Collections.sort(pvBeansList, new Comparator<PageViewsBean>() {
                public int compare(PageViewsBean o1, PageViewsBean o2) {
                  return o1.getStep()>o2.getStep()? 1:-1;//大于0表示进行升序排序
                }
            });
            // 取这次visit的首尾pageview记录，将数据放入VisitBean中
            VisitBean visitBean = new VisitBean();
            // 取visit的首记录
            visitBean.setInPage(pvBeansList.get(0).getRequest());
            visitBean.setInTime(pvBeansList.get(0).getTimestr());
            // 取visit的尾记录
            visitBean.setOutPage(pvBeansList.get(pvBeansList.size() - 1).getRequest());
            visitBean.setOutTime(pvBeansList.get(pvBeansList.size() - 1).getTimestr());
            // visit访问的页面数
            visitBean.setPageVisits(pvBeansList.size());
            // 来访者的ip
            visitBean.setRemote_addr(pvBeansList.get(0).getRemote_addr());
            // 本次visit的referal
            visitBean.setReferal(pvBeansList.get(0).getReferal());
            visitBean.setSession(key.toString());

            context.write(NullWritable.get(), visitBean);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        job.setJarByClass(ClickStreamVisit.class);

        job.setMapperClass(ClickStreamVisitMapper.class);
        job.setReducerClass(ClickStreamVisitReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewsBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(VisitBean.class);


//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("C:/weblog/pageviews"));
        FileOutputFormat.setOutputPath(job, new Path("c:/weblog/visitout"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }


}
