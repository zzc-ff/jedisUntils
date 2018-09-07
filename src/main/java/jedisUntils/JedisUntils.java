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
	// Redis������IP
	private static final String IP = "140.143.161.103";

	// Redis�Ķ˿ں�
	private static final int PORT = 6379;

	// jedispool�����ȵ�ʱ��
	private static final int TIMEOUT = 10000;

	// ��������
	private static final String AUTH = "54djePZW6MUb";

	// ���Ӻľ�ʱ�Ƿ�����, false���쳣,ture����ֱ����ʱ, Ĭ��true
	private static final boolean IS_BLOCK = true;

	// ��������ʵ���������Ŀ��Ĭ��ֵΪ8��
	// �����ֵΪ-1�����ʾ�����ƣ����pool�Ѿ�������maxActive��jedisʵ�������ʱpool��״̬Ϊexhausted(�ľ�)��
	private static final int MAX_ACTIVE = 1024;

	// ����һ��pool����ж��ٸ�״̬Ϊidle(���е�)��jedisʵ����Ĭ��ֵҲ��8��
	private static final int MAX_IDLE = 200;

	// �ȴ��������ӵ����ʱ�䣬��λ���룬Ĭ��ֵΪ-1����ʾ������ʱ����������ȴ�ʱ�䣬��ֱ���׳�JedisConnectionException��
	private static final int MAX_WAIT = 10000;

	// ������ӵ���С����ʱ�� Ĭ��1800000����(30����)
	private static final int MIN_EVICTABLEIDLE = 180000;

	// ��borrowһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�
	private static final boolean TEST_ON_BORROW = true;

	// ��returnһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�
	private static final boolean TEST_ON_RETURN = true;

	/**
	 * ��ʼ�����ݾݿ����ӳ� ���������Ϣ����
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
		System.out.println("jedispool���ӳس�ʼ��������");
	}

	/**
	 * ��ȡ�ض���
	 */
	public JedisPool getJedispool() {
		return jedispool;
	}

	/**�ӳ��л�ȡjedis���Ӷ���
	 * 
	 * @return jedispool.getResource() ����redis���ӳ����ó���jedis����
	 * 
	 * @return null ��ȡ��Դ�쳣������null
	 */
	public Jedis getResource() {
		try {
			logger.debug("��jedispool��ȡjedis");
			return jedispool.getResource();
		} catch (Exception e) {
			System.out.println("jedispool��ȡ��Դ���ӳ���");
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * ��jedis���������ӳ�
	 * 
	 * @param jedis
	 */
	public void jedisClose(Jedis jedis) {
		System.out.println("jedis �ر����ӡ�����");
		jedis.close();
	}

	/**
	 * �ر����ӳ�
	 * 
	 * @param jedispool
	 */
	public void poolClose(JedisPool jedispool) {
		System.out.println("jedispool �ر����ӳء�������");
		jedispool.close();
	}

	/**
	 * ��clusterNodes�����ڵ���Ϣ,��ȡ��nodesId
	 * 
	 * @return �ַ����� ����ȡ��nodesId���������з���
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
	 * set����
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
	 * set�������趨key��Чʱ��
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
	 * get����
	 * 
	 * @param key
	 * @param value
	 * @return key��Ӧ��values
	 */
	public String get(String key) {
		jedis = getResource();
		String valuetemp = jedis.get(key);
		jedisClose(jedis);
		return valuetemp;
	}

	/**
	 * ��pipeline ����������set
	 * 
	 * @param map
	 * @return list����
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
	 * �ܵ���������get
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
	 * keys������b���ڼ�����keys ���������ϼ�����node id
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
			System.out.println("�û���֤���󣡣�");
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
			System.out.println("�û���֤���󣡣�");
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
			System.out.println("�û���֤���󣡣�");
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
