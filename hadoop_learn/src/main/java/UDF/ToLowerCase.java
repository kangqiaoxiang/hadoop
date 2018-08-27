package UDF;


import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
/**
 * Created by Administrator on 2018/3/1.
 */
public class ToLowerCase extends UDF {


    //必须是public
    public String evaluate(String field){
        String result = field.toLowerCase();
        return result;
    }
    public String evaluate(int field){
       String str =  String.valueOf(field).substring(0,3);
      return pro.get(str)==null?"火星":pro.get(str);
    }
    public static HashMap<String,String> pro = new HashMap<String,String>();
    /**静态代码块中能写哪些东西。
     * */
    static{
        pro.put("136","北京");
        pro.put("137","上海");
        pro.put("138","武汉");
    }
}
