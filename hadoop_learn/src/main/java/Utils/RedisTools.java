package Utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**Hbase工具类
 * Created by liwenxiang on 2018/5/4
 * */

public class RedisTools {
	/**desc:properties文件获取工具类
	 * Create By
	 * 两种方法：this.getClass.getClassLoader().getResource
	 * this.getServletContext().getRealPath("")
	 * */
	
	private static JedisPool pool;//又是单例模式
	
	public static  String REDIS_IP;
	
	public static int REDIS_PORT;
	
	public static int REDIS_TIMEOUT;


	
	static{
		Properties props = new Properties();
		InputStream in = RedisTools.class.getClassLoader().getResourceAsStream("redis.properties");
		try {
			props.load(in);
			REDIS_IP = props.getProperty("redis_ip");
			REDIS_PORT = Integer.parseInt(props.getProperty("redis_port"));
			REDIS_TIMEOUT = Integer.parseInt(props.getProperty("redis_timeout"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//初始化pool，单例模式
	static{
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(1000);//最大空闲
		config.setMaxTotal(10240);//最大连接数
		if(pool == null){
			pool = new JedisPool(config,REDIS_IP, REDIS_PORT,REDIS_TIMEOUT);
		}
	}
	//获取jedis连接对象
	public static Jedis getJedis(){
		return pool.getResource();
	}
	//将jedis对象归还给连接池
	@SuppressWarnings("deprecation")
	public static void closeJedis(Jedis jedis){
		pool.returnResource(jedis);
	}
	
	/**添加String类型数据
	 * */
	public static void set(String key,String value){
		Jedis jedis = RedisTools.getJedis();
		if(jedis.exists(key)){
			jedis.del(key);//如果存在先删除
		}
		jedis.set(key, value);
		RedisTools.closeJedis(jedis);
	}
	/**获取String类型数据
	 * @param key
	 * */
	public static String get(String key){
		Jedis jedis = RedisTools.getJedis();
		String value = null;
		if(jedis.exists(key)){
			value =  jedis.get(key);
		}
		RedisTools.closeJedis(jedis);
		return value;	
	}
	/**设置List<String>类型的数据
	 * @param key
	 * @param list
	 * */
	public static Long setRList(String key,List<String> list){
		Jedis jedis = RedisTools.getJedis();
		String[] array = list.toArray(new String[list.size()]);
		Long rpush = jedis.rpush(key, array);
		RedisTools.closeJedis(jedis);
		return rpush;
	}
	/**获取List<String>的数据
	 * @param key
	 * @return
	 * */
	public static List<String> getList(String key){
		Jedis jedis = RedisTools.getJedis();
		List<String> list = new ArrayList<String>();
		long l = jedis.llen(key);
		if(l == 0){
			return null;
		}
		list = jedis.lrange(key, 0, l);
		RedisTools.closeJedis(jedis);
		return list;
	}
	
}
