package jedisUntils;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * JedisUntil����Bean
 *
 * @author zhangzicheng@rtmap.com
 * @project JedisUntils
 * @package jedisUntils
 * @date 9/13/2018
 */
public class JedisUntil {

	private static Logger logger = Logger.getLogger(JedisUntils.class);

	public final static String SCAN_POINTER_START = String.valueOf(0);
	public final static String PATH = "src/main/resources/jedis.properties";

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
	NewConnection conn = null;

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

	interface Make<E> {
		public E execute(Jedis jedis);
	}

	public <E> E execute(Make<E> executor) {
		E result;
		Jedis jedis = jedisPool.getResource();
		try {
			result = executor.execute(jedis);
		} finally {
			jedis.close();
		}
		return result;
	}

	/**
	 * key��صĲ���
	 *
	 * @param key
	 *            ��
	 * @param value
	 *            ֵ
	 * @return ���ز�����Ľ��
	 */
	Long del(String key) {
		return execute(jedis -> jedis.del(key));
	}

	Boolean exists(String key) {
		return execute(jedis -> jedis.exists(key));
	}

	Long expire(String key, int seconds) {
		return execute(jedis -> jedis.expire(key, seconds));
	}

	Long expireAt(String key, long unixTime) {
		return execute(jedis -> jedis.expireAt(key, unixTime));
	}

	String type(String key) {
		return execute(jedis -> jedis.type(key));
	}

	Long ttl(String key) {
		return execute(jedis -> jedis.ttl(key));
	}

	Long persist(String key) {
		return execute(jedis -> jedis.persist(key));
	}

	Long pexpireAt(String key, long millisecondsTimestamp) {
		return execute(jedis -> jedis.expireAt(key, millisecondsTimestamp));
	}

	Long pttl(String key) {
		return execute(jedis -> jedis.pttl(key));
	}

	String rename(String oldkey, String newkey) {
		return execute(jedis -> jedis.rename(oldkey, newkey));
	}

	Long renamenx(String oldkey, String newkey) {
		return execute(jedis -> jedis.renamenx(oldkey, newkey));
	}

	List<String> sort(String key) {
		return execute(jedis -> jedis.sort(key));
	}

	/**
	 * String ��ز���
	 *
	 * @param key
	 *            ��
	 * @param value
	 *            ֵ
	 * @return ���ز�����Ľ��
	 */
	String set(final String key, final String value) {
		return execute(jedis -> jedis.set(key, value));
	}

	String get(final String key) {
		return execute(jedis -> jedis.get(key));
	}

	String getSet(final String key, final String value) {
		return execute(jedis -> jedis.getSet(key, value));
	}

	Long decr(final String key) {
		return execute(jedis -> jedis.decr(key));
	}

	Long incrBy(final String key, final long integer) {
		return execute(jedis -> jedis.incrBy(key, integer));
	}

	Double incrByFloat(final String key, final double value) {
		return execute(jedis -> jedis.incrByFloat(key, value));
	}

	Long incr(final String key) {
		return execute(jedis -> jedis.incr(key));
	}

	Long append(final String key, final String value) {
		return execute(jedis -> jedis.append(key, value));
	}

	String substr(final String key, final int start, final int end) {
		return execute(jedis -> jedis.substr(key, start, end));
	}

	/**
	 * ����Ԫ�ص�����List��
	 *
	 * @param key
	 *            ��
	 * @param value
	 *            ֵ
	 * @return ���ͺ󼯺���Ԫ�ظ���
	 */
	public Long lpush(String key, String... value) {
		return execute(jedis -> jedis.lpush(key, value));
	}

	public Long rpush(String key, String... value) {
		return execute(jedis -> jedis.rpush(key, value));
	}

	public String lpop(String key) {
		return execute(jedis -> jedis.lpop(key));
	}

	public String rpop(String key) {
		return execute(jedis -> jedis.rpop(key));
	}

	/**
	 * set ��ز���
	 *
	 * @param key
	 *            ��
	 * @param value
	 *            ֵ
	 * @return ���ز�����Ľ��
	 */
	Long sadd(final String key, final String... members) {
		return execute(jedis -> jedis.sadd(key, members));
	}

	Set<String> smembers(final String key) {
		return execute(jedis -> jedis.smembers(key));
	}

	Long srem(final String key, final String... member) {
		return execute(jedis -> jedis.srem(key, member));
	}

	String spop(final String key) {
		return execute(jedis -> jedis.spop(key));
	}

	Set<String> spop(final String key, final long count) {
		return execute(jedis -> jedis.spop(key, count));
	}

	Long smove(final String srckey, final String dstkey, final String member) {
		return execute(jedis -> jedis.smove(srckey, dstkey, member));
	}

	Long scard(final String key) {
		return execute(jedis -> jedis.scard(key));
	}

	Boolean sismember(final String key, final String member) {
		return execute(jedis -> jedis.sismember(key, member));
	}

	/**
	 * zset ��ز���
	 *
	 * @param key
	 *            ��
	 * @param value
	 *            ֵ
	 * @return ���ز�����Ľ��
	 */

	/**
	 * hash ��ز���
	 *
	 * @param key
	 *            ��
	 * @param value
	 *            ֵ
	 * @return ���ز�����Ľ��
	 */

}