package jedisUntils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.org.apache.xerces.internal.impl.xs.SchemaGrammar.Schema4Annotations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class JedisUntils {
	private JedisPool jedispool;
	private Jedis jedis;
	//Redis服务器IP
    private static final String IP = "140.143.161.103";
   
    //Redis的端口号
    private static final int PORT = 6379;
  
    //jedispool阻塞等地时间
    private static final int TIMEOUT = 10000;
   
    //访问密码
    private static final String AUTH = "54djePZW6MUb";
    
    //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
    private static final boolean IS_BLOCK = true;

    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static final int MAX_ACTIVE = 1024;

    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static final int MAX_IDLE = 200;

    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static final int MAX_WAIT = 10000;
    
    //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
    private static final int MIN_EVICTABLEIDLE = 180000;

    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static final boolean TEST_ON_BORROW = true;
  
    //在return一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static final boolean TEST_ON_RETURN = true;

/**
 * 初始化数据据库连接池
 * 配置相关信息参数
 */
	{
		JedisPoolConfig config = new JedisPoolConfig();
		config.setBlockWhenExhausted(IS_BLOCK);
		config.setMaxTotal(MAX_ACTIVE);
		config.setMaxIdle(MAX_IDLE);
		config.setMaxWaitMillis(MAX_WAIT);
		config.setMinEvictableIdleTimeMillis(MIN_EVICTABLEIDLE);
		config.setTestOnBorrow(TEST_ON_BORROW);
		config.setTestOnReturn(TEST_ON_RETURN);
		jedispool = new JedisPool(config, IP, PORT, TIMEOUT, AUTH);
		System.out.println("jedispool连接池初始化。。。");
	}
	
	/**
	 * 获取池链接
	 */
	public JedisPool getJedispool() {
		return jedispool;
	}
	
	/**
	 * 
	 * @return jedispool.getResource() 返回redis连接池中拿出的jedis对象
	 * 
	 * @return null 获取资源异常，返回null
	 */
	public Jedis getResource() {
		System.out.println("jedis 获取连接。。。");
		try {
			return jedispool.getResource();
		} catch (Exception e) {
			System.out.println("jedispool获取资源连接出错");
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 关闭jedis
	 * @param jedis
	 */
	public void jedisClose(Jedis jedis) {
		System.out.println("jedis 关闭连接。。。");
		jedis.close();
	}
	
	/**
	 * 关闭jedispool
	 * @param jedispool
	 */
	public void poolClose(JedisPool jedispool) {
		System.out.println("jedispool 关闭连接池。。。。");
		jedispool.close();
	}
	
	/**将clusterNodes读出节点信息,截取出nodesId
	 * 
	 * @return 字符数组 将截取的nodesId放在数组中返回
	 */
	public String[] clusterNodes(){
		int start = 0;
		int NodesIdLength = 40;
		jedis = getResource();
		String nodes = jedis.clusterNodes();
		String[] nodesIdTemp = nodes.split("\n");
		String[] nodesId = new String[nodesIdTemp.length];
		for(int i =0;i<nodesIdTemp.length;i++){
			nodesId[i] = nodesIdTemp[i].substring(start,NodesIdLength);
		}
		jedisClose(jedis);
		return nodesId;
	}
	
	public String[] pipelineWithSet(Map<String,String> map){
		jedis =getResource();
		Pipeline p  = jedis.pipelined();
		Iterator it = map.entrySet().iterator();
		  while (it.hasNext()) {
		   Map.Entry entry = (Map.Entry) it.next();
		   String key = (String) entry.getKey();
		   String value = (String) entry.getValue();
		   p.set(key, value);
		  }
		
		return null;
	}
	
	
	
	/**keys命令封裝，
	 * 
	 * @param pattern
	 * @return
	 */
	
	public Set<String> keys(String pattern){
		jedis = getResource();
		StringBuffer strbuf = new StringBuffer();
		String[] nodesId = clusterNodes();
		ScanParams scanParams = new ScanParams();
		for(int i=0;i<nodesId.length;i++){
			strbuf.append(pattern+'\b'+nodesId[i]);
		}
		byte[] patter = new byte[6];
		jedis.keys(patter);
		
		return null;
	}
	
	/**
	 * 模糊匹配
	 * @param pattern key的正则表达式
	 * @param count 每次扫描多少条记录，值越大消耗的时间越短，但会影响redis性能。建议设为一千到一万
	 * @return 匹配的key集合
	    */
	public  List<String> scan(String pattern, int count){
	   List<String> list = new ArrayList<String>();

	   jedis = getResource();
	   if (jedis == null){
	      return list;
	   }

	      String cursor = ScanParams.SCAN_POINTER_START;

	      ScanOptions scanParams = new ScanOptions();
	     
	      scanParams.count(count);
	      scanParams.match(pattern);

	      do {
	         ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
	         list.addAll(scanResult.getResult());
	         cursor = scanResult.getStringCursor();
	      }while(!"0".equals(cursor));
	      
	      jedisClose(jedis);
	   return list;
	   
	}
	
}
