package cn.itcast.hive.mr.pre.cn.itcast.hive.mrbean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

/**
 * Created by Administrator on 2018/3/2.
 */
public class WebLogParser {
    //日期转换格式
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);
    //开始切分，清洗数据，按照空格来切分
    public static WebLogBean parser(String line) {
        WebLogBean webLogBean = new WebLogBean();
        String[] arr = line.split(" ");
        if (arr.length > 11) {
        webLogBean.setRemote_addr(arr[0]);
        webLogBean.setRemote_user(arr[1]);
        //substring(1)从哪里开始截取
        String time_local = formatDate(arr[3].substring(1));
        if (null == time_local) {time_local = "-invalid_time-";}
        webLogBean.setTime_local(time_local);
        webLogBean.setRequest(arr[6]);
        webLogBean.setStatus(arr[8]);
        webLogBean.setBody_bytes_sent(arr[9]);
        webLogBean.setHttp_referer(arr[10]);
        //如果useragent元素较多，拼接useragent
        if (arr.length > 12) {
            StringBuffer sb = new StringBuffer();
            for (int i = 11; i < arr.length; i++) {
                sb.append(arr[i]);
            }
            webLogBean.setHttp_user_agent(sb.toString());
        } else {
            webLogBean.setHttp_user_agent(arr[11]);
        }
        //传过来的数据都是字符串，将status转换成int
        if (Integer.parseInt(webLogBean.getStatus()) >= 400) {
            //大于400，http错误
            webLogBean.setValid(false);
        }
        if ("-invalid_time-".equals(webLogBean.getTime_local())) {
            webLogBean.setValid(false);
        }
    }else{
        webLogBean.setValid(false);
    }
        return webLogBean;
    }
    public static String formatDate(String time_local){
        try {
            return df2.format(df1.parse(time_local));
        } catch (ParseException e) {
            return null;
        }
    }
    public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
        //contains方法的查看
        if (!pages.contains(bean.getRequest())) {
            bean.setValid(false);
        }
    }
}
