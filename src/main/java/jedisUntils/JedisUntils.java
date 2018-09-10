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
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.SafeEncoder;

public class JedisUntils {

	NewConnection conn = new NewConnection();
	private static Logger logger = Logger.getLogger(JedisUntils.class);

	public final static String SCAN_POINTER_START = String.valueOf(0);
	private final static String PATH = "src/main/resources/jedis.properties";
	
	// Redis������IP
	private static final String IP = FileUntil.FileUntil.getStringProperties(PATH, "ip");
	// Redis�Ķ˿ں�
	private static final int PORT = FileUntil.FileUntil.getIntProperties(PATH, "port");
	// jedispool�����ȵ�ʱ��
	private static final int TIMEOUT = FileUntil.FileUntil.getIntProperties(PATH, "timeOut");
	// ��������
	private static final String AUTH = FileUntil.FileUntil.getStringProperties(PATH, "auth");
	// ���Ӻľ�ʱ�Ƿ�����, false���쳣,ture����ֱ����ʱ, Ĭ��true
	private static final boolean IS_BLOCK = FileUntil.FileUntil.getBooleanProperties(PATH, "isBlock");
	// ��������ʵ���������Ŀ��Ĭ��ֵΪ8��
	// �����ֵΪ-1�����ʾ�����ƣ����pool�Ѿ�������maxActive��jedisʵ�������ʱpool��״̬Ϊexhausted(�ľ�)��
	private static final int MAX_ACTIVE = FileUntil.FileUntil.getIntProperties(PATH, "maxActive");
	// ����һ��pool����ж��ٸ�״̬Ϊidle(���е�)��jedisʵ����Ĭ��ֵҲ��8��
	private static final int MAX_IDLE = FileUntil.FileUntil.getIntProperties(PATH, "maxIdle");
	// �ȴ��������ӵ����ʱ�䣬��λ���룬Ĭ��ֵΪ-1����ʾ������ʱ����������ȴ�ʱ�䣬��ֱ���׳�JedisConnectionException��
	private static final int MAX_WAIT = FileUntil.FileUntil.getIntProperties(PATH, "maxWait");
	// ������ӵ���С����ʱ�� Ĭ��1800000����(30����)
	private static final int MIN_EVICTABLEIDLE = FileUntil.FileUntil.getIntProperties(PATH, "minEvictableIdle");
	// ��borrowһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�
	private static final boolean TEST_ON_BORROW = FileUntil.FileUntil.getBooleanProperties(PATH, "testOnBrrow");
	// ��returnһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�
	private static final boolean TEST_ON_RETURN = FileUntil.FileUntil.getBooleanProperties(PATH, "testOnReturn");

	private static JedisPool jedisPool = null;

	/**
	 * ��ʼ��Redis���ӳ�
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
		logger.info("jedispool���ӳس�ʼ��������");
	}

	/**
	 * ͬ����ȡJedisʵ��
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
			logger.error("Get jedis error : " + e);
		}
		return jedis;
	}

	/**
	 * �ͷ�jedis��Դ
	 * 
	 * @param jedis
	 */
	public static void returnResource(final Jedis jedis) {
		if (jedis != null && jedisPool != null) {
			jedis.close();
		}
	}

	/**
	 * ��clusterNodes�����ڵ���Ϣ,��ȡ��nodesId
	 * 
	 * @return �ַ����� ����ȡ��nodesId���������з���
	 */
	public String[] clusterNodes() {
		int start = 0;
		Jedis jedis = getJedis();
		String nodes = jedis.clusterNodes();
		String[] nodesIdTemp = nodes.split("\n");
		String[] nodesId = new String[nodesIdTemp.length];
		for (int i = 0; i < nodesIdTemp.length; i++) {
			int iIndex = nodesIdTemp[i].indexOf(" ");
			nodesId[i] = nodesIdTemp[i].substring(start, iIndex);
		}
		returnResource(jedis);
		return nodesId;
	}

	/**
	 * set����
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
	 * set�������趨key��Чʱ��
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
	 * get����
	 * 
	 * @param key
	 * @param value
	 * @return key��Ӧ��values ���� null
	 */
	public String get(String key) {
		Jedis jedis = getJedis();
		String valuetemp = jedis.get(key);
		returnResource(jedis);
		return valuetemp;
	}

	/**
	 * ��pipeline ����������set
	 * 
	 * @param map
	 * @return list����
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
			p.expire(key, 120);
		}
		List<Object> resultList = p.syncAndReturnAll();
		return resultList;
	}

	/**
	 * �ܵ���������get
	 * 
	 * @param list
	 * @return HashMap<String, String> key value
	 * @return HashMap<String, String> key none(��ʾû�����key)
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
	 * keys������b���ڼ�����keys ���������ϼ�����node id
	 * 
	 * @param pattern
	 * @return Map<String, List<String>> ��keyû��ƥ���ʱ�� list�������������ֵΪ��
	 */
	public Map<String, List<String>> keys(String pattern) {
		Map<String, List<String>> resultMap;
		try {
			conn.sendCommand(Command.AUTH, AUTH);
			String authReply = conn.getStatusCodeReply();
			if ("ok".equalsIgnoreCase(authReply)) {
				String[] nodeArray = clusterNodes();
				List<String> keyList;
				resultMap = new HashMap<String, List<String>>();
				for (int i = 0; i < nodeArray.length; i++) {
					System.out.println(nodeArray[i]);
					conn.sendCommand(Command.KEYS, pattern, nodeArray[i]);
					keyList = conn.getMultiBulkReply();
					// System.out.println(strNodeID+":"+keyList.size());
					resultMap.put(nodeArray[i], keyList);
				}
				// conn.close();
				return resultMap;
			} else {
				System.out.println("�û��������");
			}
		} catch (JedisConnectionException jce) {
			System.out.println(jce);
		}
		conn.close();
		return null;
	}

	/**
	 * ��scan ��װ�����������ϼ�����node id ģ��ƥ�䣬����key
	 * 
	 * @return ƥ���key����
	 */
	public Map<String, List<Object>> scans() {
		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {
			String[] nodeList = clusterNodes();
			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
			for (int i = 0; i < nodeList.length; i++) {
				System.out.println(nodeList[i]);
				resultMap.put(nodeList[i], scan(nodeList[i]));
			}
			// conn.close();
			return resultMap;
		} else {
			logger.info("�û���֤���󣡣�");
			System.out.println("�û���֤���󣡣�");
			conn.close();
			return null;
		}
	}

	/**
	 * ��scan ��װ����ģ����ѯ �� ���������ϼ�����node id ģ��ƥ�䣬����key
	 * 
	 * @return ƥ���key����
	 */
	public Map<String, List<Object>> scans(String pattern) {
		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {
			String[] nodeList = clusterNodes();
			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
			for (int i = 0; i < nodeList.length; i++) {
				logger.info("�ڵ�ID:" + nodeList[i]);
				System.out.println(nodeList[i]);
				resultMap.put(nodeList[i], scan(nodeList[i], pattern));
			}
			conn.close();
			return resultMap;
		} else {
			logger.info("�û���֤���󣡣�");
			System.out.println("�û���֤���󣡣�");
			conn.close();
			return null;
		}
	}

	/**
	 * ��scan ��װ����ģ����ѯ �� ���������ϼ�����node id ģ��ƥ�䣬����key
	 * 
	 * @return ƥ���key����
	 */
	public Map<String, List<Object>> scans(int count) {

		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {
			String[] nodeList = clusterNodes();
			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
			for (int i = 0; i < nodeList.length; i++) {
				System.out.println(nodeList[i]);
				resultMap.put(nodeList[i], scan(nodeList[i], count));
			}
			conn.close();
			return resultMap;
		} else {
			logger.info("�û���֤���󣡣�");
			System.out.println("�û���֤���󣡣�");
			conn.close();
			return null;
		}
	}

	/**
	 * ��scan ��װ����ģ����ѯ ����count ���������ϼ�����node id ģ��ƥ�䣬����key
	 * 
	 * @return ƥ���key����
	 */
	public Map<String, List<Object>> scans(String pattern, int count) {
		conn.sendCommand(Command.AUTH, AUTH);
		String authReply = conn.getStatusCodeReply();
		if ("ok".equalsIgnoreCase(authReply)) {
			conn.sendCommand(Command.CLUSTER, "nodes");
			String StringNode = conn.getBulkReply();
			System.out.println("StringNode:" + StringNode);
			String[] nodeList = clusterNodes();
			Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
			for (int i = 0; i < nodeList.length; i++) {
				System.out.println(nodeList[i]);
				resultMap.put(nodeList[i], scan(nodeList[i], pattern, count));
			}
			conn.close();
			return resultMap;
		} else {
			logger.info("�û���֤���󣡣�");
			System.out.println("�û���֤���󣡣�");
			return null;
		}
	}

	/**
	 * ��������������һ��������scan������
	 * 
	 * @param node
	 *            ����scan�Ľڵ�Id
	 * 
	 * @return List����
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
		conn.close();
		return result;
	}

	/**
	 * ��������������һ��������scan������������pattern (ģ����ѯ)
	 * 
	 * @param node
	 *            ����scan�Ľڵ�Id
	 * 
	 * @return List����
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
		conn.close();
		return result;

	}

	/**
	 * ��������������һ��������scan������������count (ģ����ѯ)
	 * 
	 * @param node
	 *            ����scan�Ľڵ�Id
	 * 
	 * @return List����
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
		conn.close();
		return result;

	}

	/**
	 * ��������������һ��������scan������������pattern (ģ����ѯ) �� COUNT
	 * 
	 * @param node
	 *            ����scan�Ľڵ�Id
	 * 
	 * @return List����
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
