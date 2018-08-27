package cn.itcast.hive.mr;

import cn.itcast.hive.mr.pre.cn.itcast.hive.mrbean.WebLogBean;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**将清洗之后的日志梳理出点击流pageviews模型数据
 * Created by Administrator on 2018/3/2.
 * 输入数据是清洗过后的结果数据
 * 区分出每一次会话，给每一次visit增加了session_id
 * 梳理每一次会话中所访问的每个页面（请求页面，url,同流时长，以及该页面在这次session中的序号
 */
public class ClickStreamThree {
    static class ClickStreamMapper extends Mapper<LongWritable,Text,Text,WebLogBean>{
        //对传进来的数据进行处理
        Text k = new Text();
        WebLogBean v = new WebLogBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String line = value.toString();
           String[] fields = line.split("\001");
           if(fields.length<9) {return;}
           v.set("true".equals(fields[0]) ? true : false, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);
           //用数据中的ip做key
            k.set(v.getRemote_addr());
            context.write(k,v);
        }
    }
    static class ClickStreamReducer extends Reducer<Text,WebLogBean,NullWritable,Text> {
       Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
            /**当我们想把reducer里面的value的iterator,全取出来，包装到一个集合中时，
             * 我们就可以用BeanUtils来进行封装
             */
            ArrayList<WebLogBean> beans = new ArrayList<WebLogBean>();
            //先将一个用户的所有访问记录中的时间来出来排序
            //这种类型只取一个的代码

                for (WebLogBean bean : values) {
                    WebLogBean webLogBean = new WebLogBean();
                    try {
                        //每次只能获取一条
                        BeanUtils.copyProperties(webLogBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    beans.add(webLogBean);
                }
                /**将bean按时间先后顺序排序
                 * Arrays主要针对数组进行排序
                 * Collections主要针对集合进行排序
                 * */
                //内部类，自定义排序规则
            Collections.sort(beans, new Comparator<WebLogBean>(){

                public int compare(WebLogBean o1, WebLogBean o2) {
                    try {
                        Date d1 = toDate(o1.getTime_local());
                        Date d2 = toDate(o2.getTime_local());
                        if(d1 == null || d2 == null)
                        {return 0;}
                       return d1.compareTo(d2);//Date里面可以进行比较大小
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    return 0;
                }
            });
            /*以下逻辑为：从有序bean中分辨出各次visit,并对一次visit中所访问的page按顺序标号step
            * */
            int step = 1;
            String session = UUID.randomUUID().toString();
            for(int i = 0;i<beans.size();i++){
                WebLogBean bean = beans.get(i);
                //如果仅有一条数据，则直接输出
                if(1 == beans.size()){
                    //设置默认停留市场为60s
                    v.set(session+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001"
                            + bean.getStatus());
                    context.write(NullWritable.get(),v);
                    session = UUID.randomUUID().toString();
                    break;
                }
                //如果不止一条，则将第一条跳过不输出，遍历第二条时再输出。
                if(i==0){
                    continue;
                }
                // 求近两次时间差
                long timeDiff = 0;
                try {
                    timeDiff = timeDiff(toDate(bean.getTime_local()), toDate(beans.get(i - 1).getTime_local()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                // 如果本次-上次时间差<30分钟，则输出前一次的页面访问信息

                if (timeDiff < 30 * 60 * 1000) {

                    v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + step + "\001" + (timeDiff / 1000) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                            + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
                    context.write(NullWritable.get(), v);
                    step++;
                } else {

                    // 如果本次-上次时间差>30分钟，则输出前一次的页面访问信息且将step重置，以分隔为新的visit
                    v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + (step) + "\001" + (60) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                            + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
                    context.write(NullWritable.get(), v);
                    // 输出完上一条之后，重置step编号
                    step = 1;
                    session = UUID.randomUUID().toString();
                }
                // 如果此次遍历的是最后一条，则将本条直接输出
                if (i == beans.size() - 1) {
                    // 设置默认停留市场为60s
                    v.set(session+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
                    context.write(NullWritable.get(), v);
                }

            }

        }
        //将String类型转换成Date类型才能进行比较
        private Date toDate(String timeStr) throws ParseException {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
            return df.parse(timeStr);
        }
        private long timeDiff(String time,String time2) throws ParseException {
            Date d1 = toDate(time);
            Date d2 = toDate(time2);
            return d1.getTime()-d2.getTime();
        }
        private long timeDiff(Date time1, Date time2) throws ParseException {

            return time1.getTime() - time2.getTime();

        }

        private String toStr(Date date) {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
            return df.format(date);
        }

    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        job.setJarByClass(ClickStreamThree.class);

        job.setMapperClass(ClickStreamMapper.class);
        job.setReducerClass(ClickStreamReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//		 FileInputFormat.setInputPaths(job, new Path(args[0]));
//		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
        /*可以直接将上一步的输出目录作为这一次的输入目录
        * */
        FileInputFormat.setInputPaths(job, new Path("C:/data/output3"));
        FileOutputFormat.setOutputPath(job, new Path("C:/weblog/pageviews"));

        job.waitForCompletion(true);

    }
}
