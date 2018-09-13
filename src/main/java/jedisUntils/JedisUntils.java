package jedisUntils;

import static redis.clients.jedis.Protocol.Command.SCAN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.SafeEncoder;

public class JedisUntils {

	private static Logger logger = Logger.getLogger(JedisUntils.class);

	public final static String SCAN_POINTER_START = String.valueOf(0);
	public final static String PATH = "src/main/resources/jedis.properties";

	// Redis服务器IP
	private static final String IP = FileUntil.FileUntil.getStringProperties(PATH, "ip");
	// Redis的端口号
	private static final int PORT = FileUntil.FileUntil.getIntProperties(PATH, "port");
	// jedispool阻塞等地时间
	private static final int TIMEOUT = FileUntil.FileUntil.getIntProperties(PATH, "timeOut");
	// 访问密码
	private static final String AUTH = FileUntil.FileUntil.getStringProperties(PATH, "auth");
	// 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
	private static final boolean IS_BLOCK = FileUntil.FileUntil.getBooleanProperties(PATH, "isBlock");
	// 可用连接实例的最大数目，默认值为8；
	// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
	private static final int MAX_ACTIVE = FileUntil.FileUntil.getIntProperties(PATH, "maxActive");
	// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
	private static final int MAX_IDLE = FileUntil.FileUntil.getIntProperties(PATH, "maxIdle");
	// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
	private static final int MAX_WAIT = FileUntil.FileUntil.getIntProperties(PATH, "maxWait");
	// 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
	private static final int MIN_EVICTABLEIDLE = FileUntil.FileUntil.getIntProperties(PATH, "minEvictableIdle");
	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static final boolean TEST_ON_BORROW = FileUntil.FileUntil.getBooleanProperties(PATH, "testOnBrrow");
	// 在return一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static final boolean TEST_ON_RETURN = FileUntil.FileUntil.getBooleanProperties(PATH, "testOnReturn");

	private static JedisPool jedisPool = null;
	NewConnection conn = null;

	/**
	 * 初始化Redis连接池
	 */
	static {
		PropertyConfigurator.configure(FileUntil.FileUntil.getStringProperties(PATH, "path"));
		JedisPoolConfig config = new JedisPoolConfig();
		config.setBlockWhenExhausted(IS_BLOCK);
		config.setMaxTotal(MAX_ACTIVE);
		config.setMaxIdle(MAX_IDLE);
		config.setMaxWaitMillis(MAX_WAIT);
		config.setMinEvictableIdleTimeMillis(MIN_EVICTABLEIDLE);
		config.setTestOnBorrow(TEST_ON_BORROW);
		config.setTestOnReturn(TEST_ON_RETURN);
		jedisPool = new JedisPool(config, IP, PORT, TIMEOUT, AUTH);
		logger.info("jedispool连接池初始化。。。");
	}

	/**
	 * 同步获取Jedis实例
	 * 
	 * @return Jedis
	 */
	public static synchronized Jedis getJedis() {
		Jedis jedis = null;
		try {
			if (jedisPool != null) {
				jedis = jedisPool.getResource();
			}
		} catch (Exception e) {
			logger.error(
					"Get jedis error : " + e + "(Intenet disconnection or password is not right or port is not right)");
		}
		return jedis;
	}

	/**
	 * 同步获取Jedis实例
	 * 
	 * @return Jedis
	 */
	public static synchronized NewConnection getNewConnection() {
		return new NewConnection();
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	public static void returnResource(final Jedis jedis) {
		if (jedis != null && jedisPool != null) {
			jedis.close();
		}
	}

	/**
	 * set方法
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public String set(String key, String value) {
		Jedis jedis = getJedis();
		String valuetemp = jedis.set(key, value);
		returnResource(jedis);
		return valuetemp;
	}

	/**
	 * set方法 并设定key有效时间
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public String set(String key, String value, int times) {
		Jedis jedis = getJedis();
		String valuetemp = jedis.set(key, value);
		jedis.expire(key, times);
		returnResource(jedis);
		return valuetemp;
	}

	/**
	 * get方法
	 * 
	 * @param key
	 * @param value
	 * @return key对应的values 或者 null
	 */
	public String get(String key) {
		Jedis jedis = getJedis();
		String valuetemp = jedis.get(key);
		returnResource(jedis);
		return valuetemp;
	}

	/**
	 * 对pipeline 进行批量的set（有效时间2分钟）
	 * 
	 * @param map
	 * @return list集合
	 */
	public List<Object> pipelineWithSet(Map<String, String> map) {
		Jedis jedis = getJedis();
		Pipeline p = jedis.pipelined();
		Iterator<Entry<String, String>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			p.set(key, value);
			// p.expire(key, 120);
		}
		List<Object> resultList = p.syncAndReturnAll();
		return resultList;
	}

	/**
	 * 管道进行批量get
	 * 
	 * @param list
	 * @return HashMap<String, String> key value
	 * @return HashMap<String, String> key none(表示没有这个key)
	 */
	public HashMap<String, String> pipelineWithGet(List<String> list) {
		Jedis jedis = getJedis();
		Pipeline pipeline = jedis.pipelined();
		HashMap<byte[], Response<byte[]>> TempMap = new HashMap<byte[], Response<byte[]>>();
		for (String arg : list) {
			TempMap.put(arg.getBytes(), pipeline.get(arg.getBytes()));
		}
		pipeline.sync();
		HashMap<String, String> result = new HashMap<String, String>();
		for (Entry<byte[], Response<byte[]>> entry : TempMap.entrySet()) {
			Response<byte[]> sResponse = (Response<byte[]>) entry.getValue();
			if (sResponse.get() == null) {
				result.put(new String(entry.getKey()), "none");
			} else {
				result.put(new String(entry.getKey()), new String(sResponse.get()).toString());
			}
		}
		return result;
	}

	/**
	 * 将clusterNodes读出节点信息,截取出nodesId
	 * 
	 * @return 字符数组 将截取的nodesId放在数组中返回
	 */
	public String[] clusterNodes() {
		conn = getNewConnection();
		int start = 0;
		String[] nodesId = null;
		try {
			conn.sendCommand(Command.AUTH, AUTH);
			conn.getStatusCodeReply();
			// System.out.println(strStatus);
			conn.sendCommand(Command.CLUSTER, "nodes");
			String strNodeList = conn.getBulkReply();
			String[] nodeList = strNodeList.split("\n");
			nodesId = new String[nodeList.length];
			for (int i = 0; i < nodeList.length; i++) {
				nodesId[i] = nodeList[i].substring(start, nodeList[i].indexOf(" "));
			}
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			logger.error("get cluster nodes error");
		}
		return nodesId;
	}

	/**
	 * keys命令封裝，在检索的keys 命令后面加上检索的node id
	 * 
	 * @param pattern
	 * @return Map<String, List<String>> 当key没有匹配的时候 list集合里里里面的值为空
	 */
	public Map<String, List<String>> keys(String pattern) {
		Map<String, List<String>> resultMap;
		try {
			String[] nodeArray = clusterNodes();
			List<String> keyList;
			resultMap = new HashMap<String, List<String>>();
			for (int i = 0; i < nodeArray.length; i++) {
				// System.out.println(nodeArray[i]);
				conn.sendCommand(Command.KEYS, pattern, nodeArray[i]);
				keyList = conn.getMultiBulkReply();
				System.out.println(nodeArray[i]+":"+keyList.size());
				resultMap.put(nodeArray[i], keyList);
			}
			return resultMap;

		} catch (JedisConnectionException e) {
			e.printStackTrace();
			logger.error("get cluster nodes error");
		}
		conn.close();
		return null;
	}

	/**
	 * 对scan 封装，在命令后加上检索的node id 模糊匹配，检索key
	 * 
	 * @return 匹配的key集合
	 */
	public Map<String, List<Object>> scans() {
		String[] nodeList = clusterNodes();
		Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
		for (int i = 0; i < nodeList.length; i++) {
			// System.out.println(nodeList[i]);
			resultMap.put(nodeList[i], scan(nodeList[i]));
		}
		conn.close();
		return resultMap;
	}

	/**
	 * 对scan 封装，带模糊查询 ， 在命令后加上检索的node id 模糊匹配，检索key
	 * 
	 * @return 匹配的key集合
	 */
	public Map<String, List<Object>> scans(String pattern) {
		String[] nodeList = clusterNodes();
		Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
		for (int i = 0; i < nodeList.length; i++) {
			logger.info("节点ID:" + nodeList[i]);
			// System.out.println(nodeList[i]);
			resultMap.put(nodeList[i], scan(nodeList[i], pattern));
		}
		conn.close();
		return resultMap;
	}

	/**
	 * 对scan 封装，带模糊查询 ， 在命令后加上检索的node id 模糊匹配，检索key
	 * 
	 * @return 匹配的key集合
	 */
	public Map<String, List<Object>> scans(int count) {
		String[] nodeList = clusterNodes();
		Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
		for (int i = 0; i < nodeList.length; i++) {
			// System.out.println(nodeList[i]);
			resultMap.put(nodeList[i], scan(nodeList[i], count));
		}
		conn.close();
		return resultMap;
	}

	/**
	 * 对scan 封装，带模糊查询 ，带count 在命令后加上检索的node id 模糊匹配，检索key
	 * 
	 * @return 匹配的key集合
	 */
	public Map<String, List<Object>> scans(String pattern, int count) {
		conn.sendCommand(Command.CLUSTER, "nodes");
		String StringNode = conn.getBulkReply();
		// System.out.println("StringNode:" + StringNode);
		String[] nodeList = clusterNodes();
		Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
		for (int i = 0; i < nodeList.length; i++) {
			// System.out.println(nodeList[i]);
			resultMap.put(nodeList[i], scan(nodeList[i], pattern, count));
		}
		conn.close();
		return resultMap;
	}

	/**
	 * 这个方法用于完成一次完整的scan操作，
	 * 
	 * @param node
	 *            进行scan的节点Id
	 * 
	 * @return List集合
	 */
	private List<Object> scan(String node) {
		List<Object> result = new ArrayList<Object>();
		String cursor = String.valueOf(0);
		do {
			// scan cursor [MATCH pattern] [COUNT count]
			conn.sendCommand(Command.SCAN, cursor, node);
			result = conn.getObjectMultiBulkReply();
			cursor = new String((byte[]) result.get(0));
			// System.out.println(cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
		} while (!"0".equals(cursor));
		conn.close();
		return result;
	}

	/**
	 * 这个方法用于完成一次完整的scan操作，带参数pattern (模糊查询)
	 * 
	 * @param node
	 *            进行scan的节点Id
	 * 
	 * @return List集合
	 */
	private List<Object> scan(String node, String pattern) {
		List<Object> result = new ArrayList<Object>();
		String cursor = String.valueOf(0);
		List<byte[]> args = new ArrayList<byte[]>();
		// scan cursor [MATCH pattern] [COUNT count]
		args.add(SafeEncoder.encode(cursor));
		args.add(Protocol.Keyword.MATCH.raw);
		args.add(SafeEncoder.encode(pattern));
		args.add(SafeEncoder.encode(node));
		conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		do {
			result = conn.getObjectMultiBulkReply();
			cursor = new String((byte[]) result.get(0));
			// System.out.println(cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
			args.set(0, SafeEncoder.encode(cursor));
			conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		} while (!"0".equals(cursor));
		conn.close();
		return result;
	}

	/**
	 * 这个方法用于完成一次完整的scan操作，带参数count (模糊查询)
	 * 
	 * @param node
	 *            进行scan的节点Id
	 * 
	 * @return List集合
	 */
	private List<Object> scan(String node, int count) {
		List<Object> result = new ArrayList<Object>();
		String cursor = String.valueOf(0);
		List<byte[]> args = new ArrayList<byte[]>();
		// scan cursor [MATCH pattern] [COUNT count]
		args.add(SafeEncoder.encode(cursor));
		args.add(Protocol.Keyword.COUNT.raw);
		args.add(Protocol.toByteArray(count));
		args.add(SafeEncoder.encode(node));
		conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		do {
			result = conn.getObjectMultiBulkReply();
			cursor = new String((byte[]) result.get(0));
			// System.out.println(cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
			args.set(0, SafeEncoder.encode(cursor));
			conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		} while (!"0".equals(cursor));
		conn.close();
		return result;
	}

	/**
	 * 这个方法用于完成一次完整的scan操作，带参数pattern (模糊查询) 带 COUNT
	 * 
	 * @param node
	 *            进行scan的节点Id
	 * 
	 * @return List集合
	 */
	private List<Object> scan(String node, String pattern, int count) {
		List<Object> result = new ArrayList<Object>();
		String cursor = String.valueOf(0);
		List<byte[]> args = new ArrayList<byte[]>();
		// scan cursor [MATCH pattern] [COUNT count]
		args.add(SafeEncoder.encode(cursor));
		args.add(Protocol.Keyword.MATCH.raw);
		args.add(SafeEncoder.encode(pattern));
		args.add(Protocol.Keyword.COUNT.raw);
		args.add(Protocol.toByteArray(count));
		args.add(SafeEncoder.encode(node));
		conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		do {
			result = conn.getObjectMultiBulkReply();
			cursor = new String((byte[]) result.get(0));
			System.out.println("cusor==>" + cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
			args.set(0, SafeEncoder.encode(cursor));
			conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		} while (!"0".equals(cursor));
		conn.close();
		return result;
	}

}
