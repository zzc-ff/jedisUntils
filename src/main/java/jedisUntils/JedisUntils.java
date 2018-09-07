package jedisUntils;

import static redis.clients.jedis.Protocol.Command.SCAN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.SafeEncoder;

public class JedisUntils {
	private JedisPool jedispool;
	private Jedis jedis;
	private static Logger logger = Logger.getLogger(JedisUntils.class);
	NewConnection conn = new NewConnection();

	public final static String SCAN_POINTER_START = String.valueOf(0);
	// Redis服务器IP
	private static final String IP = "140.143.161.103";

	// Redis的端口号
	private static final int PORT = 6379;

	// jedispool阻塞等地时间
	private static final int TIMEOUT = 10000;

	// 访问密码
	private static final String AUTH = "54djePZW6MUb";

	// 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
	private static final boolean IS_BLOCK = true;

	// 可用连接实例的最大数目，默认值为8；
	// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
	private static final int MAX_ACTIVE = 1024;

	// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
	private static final int MAX_IDLE = 200;

	// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
	private static final int MAX_WAIT = 10000;

	// 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
	private static final int MIN_EVICTABLEIDLE = 180000;

	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static final boolean TEST_ON_BORROW = true;

	// 在return一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static final boolean TEST_ON_RETURN = true;

	/**
	 * 初始化数据据库连接池 配置相关信息参数
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
	 * 获取池对象
	 */
	public JedisPool getJedispool() {
		return jedispool;
	}

	/**从池中获取jedis连接对象
	 * 
	 * @return jedispool.getResource() 返回redis连接池中拿出的jedis对象
	 * 
	 * @return null 获取资源异常，返回null
	 */
	public Jedis getResource() {
		try {
			logger.debug("从jedispool获取jedis");
			return jedispool.getResource();
		} catch (Exception e) {
			System.out.println("jedispool获取资源连接出错");
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将jedis返还给连接池
	 * 
	 * @param jedis
	 */
	public void jedisClose(Jedis jedis) {
		System.out.println("jedis 关闭连接。。。");
		jedis.close();
	}

	/**
	 * 关闭连接池
	 * 
	 * @param jedispool
	 */
	public void poolClose(JedisPool jedispool) {
		System.out.println("jedispool 关闭连接池。。。。");
		jedispool.close();
	}

	/**
	 * 将clusterNodes读出节点信息,截取出nodesId
	 * 
	 * @return 字符数组 将截取的nodesId放在数组中返回
	 */
	public String[] clusterNodes() {
		int start = 0;
		int NodesIdLength = 40;
		jedis = getResource();
		String nodes = jedis.clusterNodes();
		String[] nodesIdTemp = nodes.split("\n");
		String[] nodesId = new String[nodesIdTemp.length];
		for (int i = 0; i < nodesIdTemp.length; i++) {
			nodesId[i] = nodesIdTemp[i].substring(start, NodesIdLength);
		}
		jedisClose(jedis);
		return nodesId;
	}

	/**
	 * set方法
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public String set(String key, String value) {
		jedis = getResource();
		String valuetemp = jedis.set(key, value);
		jedisClose(jedis);
		return valuetemp;
	}

	/**
	 * set方法并设定key有效时间
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public String set(String key, String value, int times) {
		jedis = getResource();
		String valuetemp = jedis.set(key, value);
		jedis.expire(key, times);
		jedisClose(jedis);
		return valuetemp;
	}

	/**
	 * get方法
	 * 
	 * @param key
	 * @param value
	 * @return key对应的values
	 */
	public String get(String key) {
		jedis = getResource();
		String valuetemp = jedis.get(key);
		jedisClose(jedis);
		return valuetemp;
	}

	/**
	 * 对pipeline 进行批量的set
	 * 
	 * @param map
	 * @return list集合
	 */
	public List<Object> pipelineWithSet(Map<String, String> map) {
		jedis = getResource();
		Pipeline p = jedis.pipelined();
		Iterator<Entry<String, String>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			p.set(key, value);
		}
		List<Object> resultList = p.syncAndReturnAll();
		return resultList;
	}

	/**
	 * 管道进行批量get
	 * 
	 * @param list
	 * @return HashMap<String, String>
	 */
	public HashMap<String, String> pipelineWithGet(List<String> list) {
		jedis = getResource();
		Pipeline pipeline = jedis.pipelined();
		HashMap<byte[], Response<byte[]>> TempMap = new HashMap<byte[], Response<byte[]>>();
		for (String arg : list) {
			TempMap.put(arg.getBytes(), pipeline.get(arg.getBytes()));
		}
		pipeline.sync();
		HashMap<String, String> result = new HashMap<String, String>();
		for (Entry<byte[], Response<byte[]>> entry : TempMap.entrySet()) {
			Response<byte[]> sResponse = (Response<byte[]>) entry.getValue();
			result.put(new String(entry.getKey()), new String(sResponse.get()).toString());
		}
		return result;
	}

	/**
	 * keys命令封裝，在检索的keys 命令后面加上检索的node id
	 * 
	 * @param pattern
	 * @return
	 */

	public Map<String, List<String>> keys(String pattern) {

		Map<String, List<String>> resultMap;
		try {
			conn.sendCommand(Command.AUTH, AUTH);

			String authReply = conn.getStatusCodeReply();

			if ("ok".equalsIgnoreCase(authReply)) {

				conn.sendCommand(Command.CLUSTER, "nodes");

				String stringNode = conn.getBulkReply();

				System.out.println("stringNode:" + stringNode);

				String[] nodeArray = stringNode.split("\n");

				List<String> keyList;

				resultMap = new HashMap<String, List<String>>();

				for (int i = 0; i < nodeArray.length; i++) {

					int iIndex = nodeArray[i].indexOf(" ");

					String strNodeID = nodeArray[i].substring(0, iIndex);

					System.out.println(strNodeID);

					conn.sendCommand(Command.KEYS, "*", strNodeID);

					keyList = conn.getMultiBulkReply();

					// System.out.println(strNodeID+":"+keyList.size());

					resultMap.put(strNodeID, keyList);
				}

				conn.close();

				return resultMap;

			} else {
				System.out.println("用户密码错误");
			}

		} catch (JedisConnectionException jce) {
			System.out.println(jce);
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

		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {

			conn.sendCommand(Command.CLUSTER, "nodes");
			String StringNode = conn.getBulkReply();
			System.out.println("StringNode:" + StringNode);
			String[] nodeList = StringNode.split("\n");

			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();

			for (int i = 0; i < nodeList.length; i++) {

				int iIndex = nodeList[i].indexOf(" ");

				String strNodeID = nodeList[i].substring(0, iIndex);

				System.out.println(strNodeID);

				resultMap.put(strNodeID, scan(strNodeID));
			}
			conn.close();
			return resultMap;
		} else {
			System.out.println("用户验证有误！！");
			return null;
		}

	}

	/**
	 * 对scan 封装，带模糊查询 ， 在命令后加上检索的node id 模糊匹配，检索key
	 * 
	 * @return 匹配的key集合
	 */
	public Map<String, List<Object>> scans(String pattern) {

		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {

			conn.sendCommand(Command.CLUSTER, "nodes");
			String StringNode = conn.getBulkReply();
			System.out.println("StringNode:" + StringNode);
			String[] nodeList = StringNode.split("\n");

			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();

			for (int i = 0; i < nodeList.length; i++) {

				int iIndex = nodeList[i].indexOf(" ");

				String strNodeID = nodeList[i].substring(0, iIndex);

				System.out.println(strNodeID);

				resultMap.put(strNodeID, scan(strNodeID, pattern));
			}
			conn.close();
			return resultMap;
		} else {
			System.out.println("用户验证有误！！");
			return null;
		}

	}

	/**
	 * 对scan 封装，带模糊查询 ， 在命令后加上检索的node id 模糊匹配，检索key
	 * 
	 * @return 匹配的key集合
	 */
	public Map<String, List<Object>> scans(int count) {

		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {

			conn.sendCommand(Command.CLUSTER, "nodes");
			String StringNode = conn.getBulkReply();
			System.out.println("StringNode:" + StringNode);
			String[] nodeList = StringNode.split("\n");

			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();

			for (int i = 0; i < nodeList.length; i++) {

				int iIndex = nodeList[i].indexOf(" ");

				String strNodeID = nodeList[i].substring(0, iIndex);

				System.out.println(strNodeID);

				resultMap.put(strNodeID, scan(strNodeID, count));
			}
			conn.close();
			return resultMap;
		} else {
			System.out.println("用户验证有误！！");
			return null;
		}

	}

	/**
	 * 对scan 封装，带模糊查询 ，带count 在命令后加上检索的node id 模糊匹配，检索key
	 * 
	 * @return 匹配的key集合
	 */
	public Map<String, List<Object>> scans(String pattern, int count) {

		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {

			conn.sendCommand(Command.CLUSTER, "nodes");
			String StringNode = conn.getBulkReply();
			System.out.println("StringNode:" + StringNode);
			String[] nodeList = StringNode.split("\n");

			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();

			for (int i = 0; i < nodeList.length; i++) {

				int iIndex = nodeList[i].indexOf(" ");

				String strNodeID = nodeList[i].substring(0, iIndex);

				System.out.println(strNodeID);

				resultMap.put(strNodeID, scan(strNodeID, pattern, count));
			}
			conn.close();
			return resultMap;
		} else {
			System.out.println("用户验证有误！！");
			return null;
		}

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
		String cursor = ScanParams.SCAN_POINTER_START;

		do {
			// scan cursor [MATCH pattern] [COUNT count]
			conn.sendCommand(Command.SCAN, cursor, node);
			result = conn.getObjectMultiBulkReply();
			cursor = new String((byte[]) result.get(0));
			System.out.println(cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
		} while (!"0".equals(cursor));
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
		String cursor = SCAN_POINTER_START;
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
			System.out.println(cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
			args.set(0, SafeEncoder.encode(cursor));
			conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		} while (!"0".equals(cursor));
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
	private List<Object> scan(String node, int count) {
		List<Object> result = new ArrayList<Object>();
		String cursor = SCAN_POINTER_START;
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
			System.out.println(cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
			args.set(0, SafeEncoder.encode(cursor));
			conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		} while (!"0".equals(cursor));
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
		String cursor = SCAN_POINTER_START;
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
			System.out.println(cursor);
			@SuppressWarnings("unchecked")
			List<byte[]> rawResults = (List<byte[]>) result.get(1);
			for (byte[] bs : rawResults) {
				result.add(SafeEncoder.encode(bs));
			}
			args.set(0, SafeEncoder.encode(cursor));
			conn.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
		} while (!"0".equals(cursor));
		return result;

	}

}
