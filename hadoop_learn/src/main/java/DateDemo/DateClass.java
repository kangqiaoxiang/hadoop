package DateDemo;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2018/3/3.
 */
public class DateClass {
    public static void main(String[] args) {
        String str = "2013-01-21 15:10:20";
        //parse()返回的是一个Date类型数据，format返回的是一个String类型数据

        /**SimpleDateFormat中的parse方法可以，把String类型的字符串转换成特定
         * 格式的date类型
         * */
        SimpleDateFormat d1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = d1.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(date);
        //把Date型的字符串转换成特定格式的String类型
        SimpleDateFormat d2 = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
        String a = d2.format(date);

        System.out.println("时间:"+a);
    }




}
